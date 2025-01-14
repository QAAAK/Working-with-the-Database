CREATE TABLE Dota_Camp (camp_id int, date date, quantity_team int, prize int, country varchar, completed boolean)

WITH (oppendonly = True, orientation = column, compresstype = zlib, compresslevel = 5)

DISTRIBUTED BY (camp_id)

PARTITION BY RANGE (date)

SUBPARTITION BY LIST(country) 
SUBPARTITION TEMPLATE (                       

    SUBPATITION europe VALUES ('europe')
    SUBPATITION europe VALUES ('usa') 
    SUBPATITION europe VALUES ('asia')
    DEFAULT SUBPARTITION other_country
)

(
    START (date '2011') INCLUSIVE
    END   (date '2024') EXCLUSIVE
    EVERY (INTERVAL '1 year')
    DEFAULT SUBPARTITION upcoming_date
    
)
