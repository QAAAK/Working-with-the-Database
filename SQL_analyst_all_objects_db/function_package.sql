select
    t.owner as schema_name
    , t.name as name
    , xmlagg(
        xmlelement(
            e 
            , t.text
            , ''
        ).extract('//text()')
        order by t.line asc
    ).getclobval() as body
    , f.wrapped
    , t.type as type
from (
    select
        owner, name, type
        , case
            when lower(text) like '%wrapped%' then 1 
            else 0
        end as wrapped
    from all_source
    where type in (
        'PACKAGE BODY'
        , 'PACKAGE'
        , 'FUNCTION'
        , 'PROCEDURE'
    )
    and line = 1 
    and owner not in ('MDSYS', 'CTXSYS', 'SYSTEM', 'XDB', 'SYS')
) f
join all_source t
on
    f.owner = t.owner
    and f.name = t.name
    and f.type = t.type
group by
    t.owner
    , t.name
    , t.type
    , f.wrapped
order by
    t.owner
    , t.name
    , t.type
;