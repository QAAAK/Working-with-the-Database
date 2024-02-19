with
filter (owner, table_name) as (
    select 'SCHEMA_NAME_1', t.*
    from table(
        sys.odcivarchar2list(
            'TABLE_NAME_1'
            , 'TABLE_NAME_2'
        )
    ) t
    union all
    select
        owner
        , table_name
    from
        all_tables
    where owner = 'SCHEMA_NAME_2'
)
, filter2 (owner, table_name) as (
    select owner, table_name 
    from all_tables
    where owner not in ('MDSYS', 'CTXSYS', 'SYSTEM', 'XDB', 'SYS')
)
, refs as (
    select
        b.constraint_type as from_constraint_type
        , b.constraint_name as from_constraint_name
        , d.position as from_position
        , d.column_name as from_column_name
        , b.table_name as from_table_name
        , b.owner as from_owner
        , a.owner as to_owner
        , a.table_name as to_table_name
        , c.column_name as to_column_name
        , c.position as to_position
        , a.constraint_name as to_constraint_name
        , a.constraint_type as to_constraint_type
    from
        all_constraints a
        left join all_constraints b
            on
                a.r_constraint_name = b.constraint_name
                and a.r_owner = b.owner
        left join all_cons_columns c
            on
                a.constraint_name = c.constraint_name
                and a.table_name = c.table_name
                and a.owner = c.owner
        left join all_cons_columns d
            on
                b.constraint_name = d.constraint_name
                and b.table_name = d.table_name
                and b.owner = d.owner
        where
            a.constraint_type = 'R'
            and b.constraint_type in ('P', 'U')
            and c.position = d.position
)
, depends as (
    select
        rtrim(
            xmlagg(
                xmlelement(
                    e
                    , to_owner || '.' || to_table_name || '.' || to_column_name
                    , ', '
                ).extract('//text()')
                order by to_owner
            ).getclobval()
            , ', '
        )  as val
        , from_owner as owner
        , from_table_name as table_name
        , from_column_name as column_name
    from refs
    where (to_owner, to_table_name) in (select * from filter2)
    group by
        from_table_name
        , from_column_name
        , from_owner
)
, impacts as (
    select
        rtrim(
            xmlagg(
                xmlelement(
                    e 
                    , from_owner || '.' || from_table_name || '.' || from_column_name
                    , ', '
                ).extract('//text()')
                order by from_owner
            ).getclobval()
            , ', '
        ) as val
        , to_owner as owner
        , to_table_name as table_name
        , to_column_name as column_name
    from refs
    where (from_owner, from_table_name) in (select * from filter2)
    group by
        to_table_name
        , to_column_name
        , to_owner
)
select
    f.owner as schema_name
    , f.table_name
    , a.column_id
    , a.column_name
    , a.data_type
    , b.comments as column_comment
    /*
        Если показатель precision не заполнен, то берется значение 38
        (максимально возможная точность в соответствии с документацией)
        , если не задан scale, то выводится значение 0 (масштаб не задан). */
    , decode (
        a.data_type
        , 'NUMBER', nvl(a.data_scale, 0)
        , ''
    ) as scale
    , decode (
        a.data_type
        , 'NUMBER', nvl(a.data_precision, 38)
        , ''
    ) as precision
    /*
        По умолчанию длина строки для типов CHAR, VARCHAR2 и их псевдонимов
        в DDL-скриптах задается в байтах, а для типов NCHAR or NVARCHAR2
        в символах.*/
    , a.data_length as byte_length
    , case
        when a.data_type in ('CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2')
        then d.value
    end as encoding
    , case
        when a.data_type in ('CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2')
        then a.char_length --a.char_col_decl_length
    end as char_length
    , decode(a.nullable, 'Y', 'N', 'Y') as not_null
    , decode(c.is_primary, 1, 'Y', 'N') as is_primary
    , case when a.virtual_column = 'NO' then a.data_default
        else null end
    as default_value
    , impacts.val as column_impact
    , depends.val as column_depend
    , decode(a.virtual_column, 'YES', 'Y', 'NO', 'N', null) as is_calculated
    , case when a.virtual_column = 'YES' then a.data_default
        else null end
    as algorithm
from
    filter f
    left join all_tab_cols a
        on
            f.owner = a.owner
            and f.table_name = a.table_name
    left join all_col_comments b
        on
            a.owner = b.owner
            and a.table_name = b.table_name
            and a.column_name = b.column_name
    left join (
        select
            1 as is_primary
            , owner
            , table_name
            , column_name
        from all_cons_columns
        where (owner, constraint_name) in (
            select owner, constraint_name
            from all_constraints
            where constraint_type = 'P'
        )
    ) c
        on
            a.owner = c.owner
            and a.table_name = c.table_name
            and a.column_name = c.column_name
    left join v$nls_parameters d
        on decode (
            a.character_set_name
            , 'CHAR_CS', 'NLS_CHARACTERSET'
            , 'NCHAR_CS', 'NLS_NCHAR_CHARACTERSET'
            , a.character_set_name
        ) = d.parameter
    left join depends
        on
            a.owner = depends.owner
            and a.table_name = depends.table_name
            and a.column_name = depends.column_name
    left join impacts
        on
            a.owner = impacts.owner
            and a.table_name = impacts.table_name
            and a.column_name = impacts.column_name
order by
    f.owner
    , f.table_name
    , a.column_id
;