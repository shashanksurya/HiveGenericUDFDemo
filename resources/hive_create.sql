
#CREATE HIVE TABLE#
#>>>------------>#
CREATE TABLE sse2 (
  age int,
  name string,
  position string,
  salary int,
  skills array<string>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
'serialization.format' = '1','paths'='name, age, position,salary,skills'
)LOCATION 's3://nordstrom-sse2/';
  
#CREATE HIVE FUNCTION FROM JAR#
#>>>-------------------------->#
 
 
CREATE TEMPORARY FUNCTION decryptkms as 'edu.shashank.HiveUDFDemo.KMSDecrypt';
  
#SELECT QUERY USING TEMPORARY FUNCTION#
#>>>---------------------------------->#
 
SELECT decryptkms(position) from sse2;