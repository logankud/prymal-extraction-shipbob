CREATE EXTERNAL TABLE IF NOT EXISTS shopify_qty_sold_by_sku_daily(
, order_date DATE
, sku_name STRING
, qty_sold INT

)
PARTITIONED BY 
(
partition_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/shopify/shopify_qty_sold_by_sku_daily/'