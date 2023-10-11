CREATE EXTERNAL TABLE IF NOT EXISTS shipbob_inventory(

id INT
,name STRING
,total_fulfillable_quantity INT
, total_onhand_quantity INT
,total_committed_quantity INT
, total_sellable_quantity INT
,total_awaiting_quantity INT
, total_exception_quantity INT
,total_internal_transfer_quantity INT
, total_backordered_quantity INT
, is_active BOOLEAN

)
PARTITIONED BY 
(
partition_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/shipbob/shipbob_inventory/'