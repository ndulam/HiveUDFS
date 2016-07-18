start-dfs.sh  //Starts HDFS Service
start-yarn-sh // Starts Yarn Cluster Manager
hiveserver2 & // Starts Hive Server2

OR Use Managed Services like CDH, HDP, AWS, Qubole, Databricks.


https://github.com/rcongiu/Hive-JSON-Serde
<property>
  <name>hive.aux.jars.path</name>
  <value>earlier-jars,file:///usr/lib/hive/lib/json-serde.jar</value>
</property>

add jar /home/cloudera/training/hive/lib/json-serde.jar

CREATE EXTERNAL TABLE IF NOT EXISTS yelpbusiness (
business_id STRING,
full_address STRING,
hours MAP<STRING, MAP<STRING, STRING>>,  
open STRING,
categories ARRAY<STRING>,
city STRING,
review_count INT,
name STRING,
neighborhoods ARRAY<STRING>,
longitude DOUBLE,
state STRING,
stars DOUBLE,
latitude DOUBLE,
attributes MAP<STRING, STRING>,
type STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hduser/data/yelp/business';

ALTER TABLE yelpbusiness SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");

SELECT * from yelpbusiness;

Total reviews in the dataset
SELECT sum(review_count) as totalreviews FROM yelpbusiness;


Top states and cities in total number of reviews
select state, city, count(*) totalreviews from yelpbusiness group by state, city order by count(*) desc limit 10;

Average number of reviews per business star rating
select stars,trunc(avg(review_count)) reviewsavg from yelpbusiness group by stars order by stars desc;

Top businesses with high review counts (> 1000)
select name, state, city, `review_count` from yelpbusiness where review_count > 1000 order by `review_count` desc limit 10;

Number of restaurants in the data set
select count(*) as TotalRestaurants from yelpbusiness where true=repeated_contains(categories,'Restaurants');

Top restaurants in number of reviews
select name,state,city,`review_count` from yelpbusiness where true=repeated_contains(categories,'Restaurants') order by `review_count` desc limit 10;

Top restaurants in number of listed categories
select name,repeated_count(categories) as categorycount, categories from yelpbusiness where true=repeated_contains(categories,'Restaurants') order by repeated_count(categories) desc limit 10;

Top first categories in number of review counts
select categories[0], count(categories[0]) as categorycount from yelpbusiness group by categories[0] order by count(categories[0]) desc limit 10;



Yelp Reviews:
CREATE EXTERNAL TABLE IF NOT EXISTS yelpreviews (
votes STRUCT<funny:INT, useful:INT, cool:INT>,
user_id STRING,
review_id STRING,
stars DOUBLE,
date STRING,
text STRING,
type STRING,
business_id STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hduser/data/yelp/reviews';
ALTER TABLE YelpBusiness SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");


select * from yelpreviews;


Top businesses with cool rated reviews
select b.name from yelpbusiness b where b.business_id in 
                  (select r.business_id 
                   from yelpreviews r 
                   group by r.business_id having sum(r.votes.cool) > 2000 
                   order by sum(r.votes.cool)  desc);


SELECT name, city, state, SUM(votes.cool) AS coolness
FROM yelpreviews r JOIN yelpbusiness b
ON (r.business_id = b.business_id)
WHERE array_contains(categories, 'Restaurants')
GROUP BY state, city, name
ORDER BY coolness DESC
LIMIT 25;




//Managed Table
CREATE TABLE top_cool AS
SELECT name, city, state, SUM(votes.cool) AS coolness, '2014-01-08' as `date`
FROM yelpreviews r JOIN yelpbusiness b
ON (r.business_id = b.business_id)
WHERE array_contains(categories, 'Restaurants')
AND `date` = '2014-01-08'
GROUP BY state, city
ORDER BY coolness DESC
LIMIT 10;


//External Table
CREATE EXTERNAL TABLE topn_cool (
name STRING,
city STRING,
state STRING,
coolness INT,
date STRING)
LOCATION '/user/hduser/hive/yelp/topN';

INSERT OVERWRITE TABLE topn_cool SELECT name, city, state, SUM(votes.cool) AS coolness, '2014-01-08' as `date`
FROM yelpreviews r JOIN yelpbusiness b
ON (r.business_id = b.business_id)
WHERE array_contains(categories, 'Restaurants')
AND `date` = '2014-01-08'
GROUP BY state, city, name
ORDER BY coolness DESC
LIMIT 10;


INSERT INTO TABLE topn_cool SELECT name, city, state, SUM(votes.cool) AS coolness, '2014-01-09' as `date`
FROM yelpreviews r JOIN yelpbusiness b
ON (r.business_id = b.business_id)
WHERE array_contains(categories, 'Restaurants')
AND `date` = '2014-01-09'
GROUP BY state, city, name
ORDER BY coolness DESC
LIMIT 10;


CREATE TABLE top_cool_hbase (
key string,
value map<string, int>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, review:")
TBLPROPERTIES ("hbase.table.name" = "top_cool");

ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hbase-handler-0.12.0-cdh5.0.3.jar;
ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hbase-client.jar;
ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hbase-common.jar;
ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hbase-protocol.jar;
ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hbase-server.jar;
ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/zookeeper.jar;
ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/guava-11.0.2.jar;

INSERT OVERWRITE TABLE top_cool_hbase SELECT name, map(`date`, cast(coolness as int)) FROM topn_cool;

INSERT OVERWRITE TABLE top_cool_hbase SELECT name, map(`date`, cast(r.stars as int)) FROM yelpreviews r join yelpbusiness b on r.business_id = b.business_id;

