CREATE EXTERNAL TABLE {database-name}.{table-name}(
  voyage_status_sk int,
  aviation_schedule_status_id varchar(64),
  carrier_md_code varchar(100),
  carrier_name varchar(250),
  voyage_number varchar(255),
  departure_port_md_code varchar(100),
  arrival_port_md_code varchar(100),
  inbound_outbound varchar(25),
  ssm_std_datetime_utc timestamp,
  ssm_std_datetime_uk timestamp,
  pax_flight varchar(100),
  ssm_atd_datetime_utc timestamp,
  ssm_atd_datetime_uk timestamp,
  ssm_ata_datetime_utc timestamp,
  ssm_ata_datetime_uk timestamp,
  cv_score_pax_ci int,
  cv_score_pax_dc int,
  pax_ci_nbtc_violation_flag int,
  pax_strikethrough_flag int,
  pax_strikethrough_datetime_uk timestamp,
  pax_strikethrough_reason varchar(100),
  cv_score_crew_ci int,
  cv_score_crew_dc int,
  crew_ci_nbtc_violation_flag int,
  crew_strikethrough_flag int,
  crew_strikethrough_datetime_uk timestamp,
  crew_strikethrough_reason varchar(100),
  dv_load_dt timestamp,
  ssm_etd_datetime_utc timestamp,
  ssm_etd_datetime_uk timestamp,
  ssm_eta_datetime_utc timestamp,
  ssm_eta_datetime_uk timestamp,
  destination_status varchar(64),
  origin_status varchar(64),
  ssm_sta_datetime_utc timestamp,
  ssm_sta_datetime_uk timestamp,
  pax_ci_nbtc_violation_datetime_uk timestamp,
  crew_ci_nbtc_violation_datetime_uk timestamp,
  carrier_iata_code varchar(100),
  departure_port_name varchar(250),
  off_block varchar(64),
  out varchar(64),
  down varchar(64),
  on_block varchar(64),
  cta_arrival_port int,
  cta_departure_port int,
  departure_confirmation_type varchar(200),
  arrival_port_name varchar(250),
  id varchar(100),
  update_code varchar(1),
  rpt_version int,
  rpt_load_dt timestamp,
  voyage_status_pipeline_key int)
PARTITIONED BY (
  path_name string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '{bucket-name}/{table-name}/table/{target-path-name}/';
