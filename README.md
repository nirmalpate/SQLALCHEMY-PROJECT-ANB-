# SQLALCHEMY-PROJECT-ANB-
THIS PROJECT IS ABOUT NETFLIX TITLE ANALYSIS. 
I HAVE EXTRACTED THE DATA FROM VARIOUS SOURCES THEN I HAVE UPLOADED IN PYTHON. AFTER THAT I HAVE TRANSFERRED THE DATA TO MYSQL VIA SQLALCHEMY.
THEN I HAVE RESOLVED FIVE QUESTIONS. 
TOOLS : PYTHON, MYSQL

CODE:
PYTHON:
import pandas as pd
import numpy as np

df = pd.read_csv("C:/Users/ADM/Desktop/netflix_titles.csv")

df.info()

from sqlalchemy import create_engine

db_user = 'root'
db_password = '0000'
db_host = 'localhost'
db_port = '3306'
db_name = 'pop'

connection_string = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

engine = create_engine(connection_string)

engine

df.to_sql('data',engine, if_exists = 'replace', index = False)

df.isnull().sum()


MYSQL CODE:

SELECT  director, country, count(*) as freq
FROM pop.data
where director is not null and country is not null
group by director, country
order by count(*) desc;


create temporary table direct as
select director, country
from(
		 select director, country,row_number() over(partition by director order by count(*) desc) as rnk
         from data
         where country is not null and director is not null
         group by director, country) t
         where rnk = 1;
         
         
         update data y
         join direct dc on y.director = dc.director
         set y.country = dc.country
         where y.country is null;
         
         select  type, count(show_id) as total
         from data
         group by type;
         
         select   country , count(show_id) as total
         from data
         where country is not null
         group by country;

 MYSQL :

 SELECT  release_year, director, count(*) as cn
FROM pop.data
where director is not null
group by release_year, director;

with cte as(
select director, release_year,count(show_id) over(partition by release_year   ) as rnk
from pop.data)
select *
from cte
order by rnk desc;

select director, release_year,count(*) as total
from pop.data
where director is not null
group by director,release_year;

with cte as(
select director, release_year, count(show_id) over(partition by  release_year) as rnk
from pop.data
where director is not null
 ), mm as(
 select  *, row_number() over(partition by release_year order by rnk desc) as nn
 from cte),cte5 as(
 select  director, release_year,max(nn) as sd
 from mm
 group by director, release_year
 ), loopa as(
 select *, max(sd) over(partition by release_year ) as n
 from cte5)
 select *
 from loopa
 where sd = n


 MYSQL:

 with comedies as(SELECT * 
FROM pop.data
where listed_in like "Comedies" and director is not null)
select  country, count(show_id) as total_movies
from comedies
group by country
limit 1;



select  listed_in, avg(duration) as avg_duration
from pop.data
where director is not null and type = "Movie"
group by listed_in;

with cte as(
select *,replace(duration, ' min', ' ' )   as duration1
from pop.data
where type = "Movie")
select listed_in, avg(duration) as du
from cte
group by listed_in;


select *, row_number() OVER(partition by  listed_in,DIRECTOR) AS RNK
from pop.data
WHERE LISTED_IN LIKE "HORROR MOVIES" OR LISTED_IN LIKE "COMEDIES" AND DIRECTOR IS NOT NULL;

with CTE AS(
SELECT  DIRECTOR, LISTED_IN,COUNT(*) AS TOTAL
FROM POP.DATA
WHERE DIRECTOR IS NOT NULL AND LISTED_IN LIKE "HORROR MOVIES" OR LISTED_IN LIKE "COMEDIES"
GROUP BY DIRECTOR,LISTED_IN
)
SELECT *
FROM CTE
WHERE DIRECTOR = "ADAM McKay";


 
