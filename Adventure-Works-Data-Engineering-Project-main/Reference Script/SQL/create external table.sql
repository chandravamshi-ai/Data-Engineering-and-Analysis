 CREATE MASTER KEY ENCRYPTION BY PASSWORD = '23987hxJ#KL95234nl0zBe'; 


CREATE DATABASE SCOPED CREDENTIAL adv_work
WITH IDENTITY = 'Managed Identity';


CREATE EXTERNAL DATA SOURCE source_silver
 WITH
  ( LOCATION='https://learnazuredatalake.blob.core.windows.net/silver',
    CREDENTIAL = adv_work
  );


CREATE EXTERNAL DATA SOURCE source_gold
 WITH
  ( LOCATION='https://learnazuredatalake.blob.core.windows.net/gold',
    CREDENTIAL = adv_work
  );

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'

);

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales;


SELECT * FROM gold.extsales;
