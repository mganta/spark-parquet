DROP table hotel_taxes;
CREATE EXTERNAL TABLE hotel_taxes (
   tax_payer_number String,
   tax_payer_name String,
   tax_payer_address String,
   tax_payer_city String,
   tax_payer_state String,
   tax_payer_zipcode String,
   tax_payer_county String,
   outlet_number int,
   location_name string,
   location_address string,
   location_city string,
   location_state string,
   location_zipcode int,
   location_county string,
   location_room_capacity int,
   location_room_receipts float,
   location_taxable_receipts float
)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
LOCATION '/user/<username>/<outdir>';