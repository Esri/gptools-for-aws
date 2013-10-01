add jar /home/hadoop/gis-tools/lib/esri-geometry-api-1.1.1.jar;
add jar /home/hadoop/gis-tools/lib/spatial-sdk-hive-1.0.1.jar;

create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';

DROP TABLE wimg08;
CREATE EXTERNAL TABLE wimg08(RequestID INT, Date STRING, Time STRING, IP STRING, Protocol INT, Method INT, Scale INT, TileRow INT, TileColumn INT, URI STRING, Status INT, Bytes INT, TimeTaken INT, Referer STRING, UserAgent STRING, Cookie STRING, DateNumber INT) 
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 's3://marwa.gishadoop.com/sample/data/sample2';

alter table wimg08 recover partitions;


DROP TABLE HeatMapTemp;
CREATE TABLE HeatMapTemp AS 
Select RequestID, floor(TileRow/64) as TileRow, floor(TileColumn/64) as TileColumn from wimg08 where Scale = 19;

INSERT INTO TABLE HeatMapTemp 
Select RequestID, floor(TileRow/32) as TileRow, floor(TileColumn/32) as TileColumn from wimg08 where Scale = 18;

INSERT INTO TABLE HeatMapTemp 
Select RequestID, floor(TileRow/16) as TileRow, floor(TileColumn/16) as TileColumn from wimg08 where Scale = 17;

INSERT INTO TABLE HeatMapTemp 
Select RequestID, floor(TileRow/8) as TileRow, floor(TileColumn/8) as TileColumn from wimg08 where Scale = 16;

INSERT INTO TABLE HeatMapTemp 
Select RequestID, floor(TileRow/4) as TileRow, floor(TileColumn/4) as TileColumn from wimg08 where Scale = 15;

INSERT INTO TABLE HeatMapTemp 
Select RequestID, floor(TileRow/2) as TileRow, floor(TileColumn/2) as TileColumn from wimg08 where Scale = 14;

INSERT INTO TABLE HeatMapTemp 
Select RequestID, floor(TileRow) as TileRow, floor(TileColumn) as TileColumn from wimg08 where Scale = 13;



DROP TABLE HeatMapTemp2;
CREATE TABLE HeatMapTemp2 AS
Select MIN(RequestID) as ObjectID, TileRow, TileColumn, count(*) as TileCount from HeatMapTemp
Group by TileRow, TileColumn Order by TileRow, TileColumn;


DROP TABLE HeatMapFinal;
CREATE TABLE HeatMapFinal AS
Select ObjectID, TileRow, TileColumn, TileCount, 
(-20035062.357882 + (4891.969810 * TileColumn)) as longitude, (20035062.357882 - (4891.969810 * TileRow)) as latitude 
from HeatMapTemp2 
where TileRow <= 8591 and TileRow >= 0 and TileColumn <= 8191 and TileColumn >= 0;

DROP TABLE heatmapfinalout;
CREATE external TABLE heatmapfinalout (ObjectID int, TileRow int, TileColumn int, TileCount int, longitude Double, latitude Double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '${OUTPUT}';


INSERT OVERWRITE TABLE heatmapfinalout SELECT * from HeatMapFinal;

