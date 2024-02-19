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
    f.owner as schema_name
    , f.table_name
    , p.part_key
    , pc.partition_name
    , pc.partition_position
    , pc.num_rows as partition_height
    , s.subpart_key
    , sc.subpartition_name
    , sc.subpartition_position
    , sc.num_rows as subpartition_height
from
    filter f
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
            f.owner = p.owner
            and f.table_name = p.table_name
    left join all_tab_partitions pc
        on
            p.table_name = pc.table_name
            and p.owner = pc.table_owner
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
            p.owner = s.owner
            and p.table_name = s.table_name
    left join all_tab_subpartitions sc
        on
            f.owner = sc.table_owner
            and f.table_name = sc.table_name
            and pc.partition_name = sc.partition_name
    order by
        f.owner
        , f.table_name
;