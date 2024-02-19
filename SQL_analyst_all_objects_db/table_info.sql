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
select 
    a.owner as schema_name
    , a.table_name
    , e.comments
    , b.height
    , c.width
    , d.datetime_columns
    , b.avg_row_len
    , p.part_key
    , s.subpart_key
from
    filter a
    left join (
        select 
            owner
            , table_name
            , num_rows as height
            , avg_row_len
        from all_tab_statistics
        where object_type = 'TABLE'
    ) b
        on
            a.table_name = b.table_name
            and a.owner = b.owner
    left join (
        select
            owner
            , table_name
            , count(1) as width
        from all_tab_columns
        group by
            owner
            , table_name
    ) c
        on
            a.table_name = c.table_name
            and a.owner = c.owner
    left join (
        select
            owner
            , table_name
            , listagg(
                column_name || ' (' || data_type || ')'
                , ', '
            ) within group (order by column_id) as datetime_columns
        from all_tab_columns 
        where
            data_type = 'DATE'
            or data_type like 'TIMESTAMP%'
            or data_type like 'INTERVAL%'
            or lower(column_name) like '%period%'
            or lower(column_name) like '%date%'
            or lower(column_name) like '%time%'
        group by
            owner
            , table_name
    ) d
        on
            a.table_name = d.table_name
            and a.owner = d.owner
    left join (
        select
            owner
            , table_name
            , comments
        from all_tab_comments
        where table_type = 'TABLE'
    ) e
        on
            a.table_name = e.table_name
            and a.owner = e.owner
    left join (
        select
            owner
            , name as table_name
            , listagg(
                column_name
                , ', '
            ) within group (order by column_position) as part_key
        from all_part_key_columns
        where object_type = 'TABLE'
        group by
            owner
            , name
    ) p
        on 
            a.owner = p.owner
            and a.table_name = p.table_name
    left join (
        select
            owner
            , name as table_name
            , listagg(
                column_name
                , ', '
            ) within group (order by column_position) as subpart_key
        from all_subpart_key_columns
        where object_type = 'TABLE'
        group by
            owner
            , name
    ) s
        on
            a.owner = s.owner
            and a.table_name = s.table_name
order by
    e.owner
    , e.table_name
;