# #! /bin/python


# ''' ******************************************************************************
        # PROGRAM_NAME:  aircraftstream.py
        # AUTHOR          :       Subhadra Tummalapally
        # DESCRIPTION     :       This spark script will transforms raw avaitioan data and generate pubslihed data
        # ARGUMENTS:
                # -e | --
               
               
        # REVISION HISTORY:
        # DATE            AUTHOR                          REASON FOR CHANGE
        # ----------------------------------------------------------------------------
        # 28/12/2020      Subhadra Tummalapally                   US: Created
		# 
        # ----------------------------------------------------------------------------


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SQLContext
import boto3
import datetime
import pytz
from pytz import timezone
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
aws_region = 'us-east-2'
glue = boto3.client('glue', region_name=aws_region)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



now = datetime.datetime.now(pytz.timezone('US/Eastern'))
dt = now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + "_" + now.strftime(
    "%H") + "_" + now.strftime("%M") + "_" + now.strftime("%S")

dt_day = now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d")
  
time_list = ['16','17']
target_file = 'aircraftdata'
s3_dbname = 'default'

glue_temp_storage = "s3://aviationoutputdata/stream_to_orc_data/aws_temp_files"
glue_relationalize_output_s3_path = "s3://aviationoutputdata/stream_to_orc_data/orc_output_files"
dfc_root_table_name = "root" #default value is "roottable"
s3_target = "s3://published-data-aviation-cds/aircraftdata/"


for t in time_list:

    s3_location = "s3://raw-aviationdata-cds/aircraftdata/2021/01/11/"+t+"/"
    
    datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": [s3_location]
            },
            format="Json",
            transformation_ctx="datasource0")
    dfc = Relationalize.apply(frame = datasource0, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "dfc")
    dynamicframe = dfc.select('root_prop').toDF()
    dynamicframe.createOrReplaceTempView("aircraftdata")
    
    df_data = dynamicframe.withColumnRenamed("prop.val.iata_code","iata_code")\
                          .withColumnRenamed("prop.val.aircraft_name", "aircraft_name")
    
    df_data.write.mode('overwrite').parquet(s3_target+'/'+dt_day)
    
    ###################Airplane data loading into parquet##################
    
    s3_location = "s3://raw-aviationdata-cds/airplanedata/2021/01/11/"+t+"/"
    s3_target = "s3://published-data-aviation-cds/airplanedata/"
    
    datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": [s3_location]
            },
            format="Json",
            transformation_ctx="datasource0")
    dfc = Relationalize.apply(frame = datasource0, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "dfc")
    dynamicframe_airplane = dfc.select('root_prop').toDF()
    
    df_airplane = dynamicframe_airplane.withColumnRenamed("prop.val.iata_type", "iata_type")\
                                      .withColumnRenamed("prop.val.airline_iata_code", "airplane_iata_code")\
                                      .withColumnRenamed("prop.val.iata_code_short", "iata_code_short")\
                                      .withColumnRenamed("prop.val.iata_code_short", "iata_code_short")\
                                      .withColumnRenamed("prop.val.construction_number","construction_number")\
                                      .withColumnRenamed("prop.val.delivery_date", "delivery_date")\
                                      .withColumnRenamed("prop.val.engines_count", "engines_count")\
                                      .withColumnRenamed("prop.val.engines_type", "engines_type")\
                                      .withColumnRenamed("prop.val.first_flight_date", "first_flight_date")\
                                      .withColumnRenamed("prop.val.icao_code_hex", "icao_code_hex")\
                                      .withColumnRenamed("prop.val.line_number", "line_number")\
                                      .withColumnRenamed("prop.val.model_code","model_code")\
                                      .withColumnRenamed("prop.val.registration_number","registration_number")\
                                      .withColumnRenamed("prop.val.test_registration_number", "test_registration_number")\
                                      .withColumnRenamed("prop.val.plane_age", "plane_age")\
                                      .withColumnRenamed("prop.val.model_name", "model_name")\
                                      .withColumnRenamed("prop.val.plane_owner", "plane_owner")\
                                      .withColumnRenamed("prop.val.plane_series", "plane_series")\
                                      .withColumnRenamed(" prop.val.plan", "plan")\
                                      .withColumnRenamed("prop.val.production_line", "production_line")\
                                      .withColumnRenamed("prop.val.registration_date", "registration_date")\
                                      .withColumnRenamed("prop.val.rollout_date", "rollout_date")
    df_airplane.write.mode('overwrite').parquet(s3_target+'/'+dt_day)
     

        ######################citidata#######################################
    
    s3_location = "s3://raw-aviationdata-cds/citidata/2021/01/11/"+t+"/"
    s3_target = "s3://published-data-aviation-cds/citidata/"
    
    datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": [s3_location]
            },
            format="Json",
            transformation_ctx="datasource0")
    dfc = Relationalize.apply(frame = datasource0, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "dfc")
    dynamicframe_citidata = dfc.select('root_prop').toDF()
    df_citidata = dynamicframe_citidata.withColumnRenamed("prop.val.gmt", "gm")\
                                      .withColumnRenamed("prop.val.iata_code", "iata_code")\
                                      .withColumnRenamed("prop.val.country_iso2", "country_iso2")\
                                      .withColumnRenamed("prop.val.geoname_id", "geoname_id")\
                                      .withColumnRenamed("prop.val.latitude", "latitude")\
                                      .withColumnRenamed("prop.val.city_name", "city_name")\
                                      .withColumnRenamed("prop.val.timezone", "timezone")
                                       
    df_citidata.write.mode('overwrite').parquet(s3_target+'/'+dt_day)
    
    ###################filght_data#############################
    
    s3_location = "s3://raw-aviationdata-cds/flightdata/2021/01/11/"+t+"/"
    s3_target = "s3://published-data-aviation-cds/flightdata/"
    
    datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": [s3_location]
            },
            format="Json",
            transformation_ctx="datasource0")
    dfc = Relationalize.apply(frame = datasource0, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "dfc")
    dynamicframe = dfc.select('root_prop').toDF()
    
    df_flightdata = dynamicframe.withColumnRenamed("prop.val.flight_date","flight_date")\
                                .withColumnRenamed("prop.val.flight_status", "flight_status")\
                                .withColumnRenamed("prop.val.departure.airport","departure_airport")\
                                .withColumnRenamed("prop.val.departure.timezone", "departure_timezone")\
                                .withColumnRenamed("prop.val.departure.iata", "departure_iata")\
                                .withColumnRenamed("prop.val.departure.icao", "departure_icao")\
                                .withColumnRenamed("prop.val.departure.terminal", "departure_terminal")\
                                .withColumnRenamed("prop.val.departure.gate", "departure_gate")\
                                .withColumnRenamed("prop.val.departure.scheduled", "departure_scheduled")\
                                .withColumnRenamed("prop.val.departure.estimated", "departure_estimated")\
                                .withColumnRenamed("prop.val.departure.actual", "departure_actual")\
                                .withColumnRenamed("prop.val.departure.estimated_runway", "departure_estimated_runway")\
                                .withColumnRenamed("prop.val.departure.actual_runway", "departure_actual_runway")\
                                .withColumnRenamed("prop.val.arrival.airport", "arrival_airport")\
                                .withColumnRenamed("prop.val.arrival.timezone", "arrival_timezone")\
                                .withColumnRenamed("prop.val.arrival.iata", "arrival_iata")\
                                .withColumnRenamed("prop.val.arrival.icao", "arrival_icao")\
                                .withColumnRenamed("prop.val.arrival.terminal", "arrival_terminal")\
                                .withColumnRenamed("prop.val.arrival.gate", "arrival_gate")\
                                .withColumnRenamed("prop.val.arrival.baggage", "arrival_baggage")\
                                .withColumnRenamed("prop.val.arrival.scheduled", "arrival_scheduled")\
                                .withColumnRenamed("prop.val.arrival.estimated", "arrival_estimated")\
                                .withColumnRenamed("prop.val.arrival.actual", "arrival_actual")\
                                .withColumnRenamed("prop.val.arrival.estimated_runway", "arrival_estimated_runway")\
                                .withColumnRenamed("prop.val.arrival.actual_runway", "arrival_actual_runway")\
                                .withColumnRenamed("prop.val.airline.name", "airline_name")\
                                .withColumnRenamed("prop.val.airline.iata", "airline_iata")\
                                .withColumnRenamed("prop.val.airline.icao", "airline_icao")\
                                .withColumnRenamed("prop.val.flight.number", "flight_number")\
                                .withColumnRenamed("prop.val.flight.iata", "flight_iata")\
                                .withColumnRenamed("prop.val.flight.icao", "flight_icao")\
                                .withColumnRenamed("prop.val.flight.codeshared.airline_name", "flight_codeshared_airline_name")\
                                .withColumnRenamed("prop.val.flight.codeshared.airline_iata", "flight_codeshared_airline_iata")\
                                .withColumnRenamed("prop.val.flight.codeshared.airline_icao", "flight_codeshared_airline_icao")\
                                .withColumnRenamed("prop.val.flight.codeshared.flight_number", "flight_codeshared_flight_number")\
                                .withColumnRenamed("prop.val.flight.codeshared.flight_iata", "flight_codeshared_flight_iata")\
                                .withColumnRenamed("prop.val.flight.codeshared.flight_icao", "flight_codeshared_flight_icao")\
                                .withColumnRenamed("prop.val.aircraft.registration", "aircraft_registration")\
                                .withColumnRenamed("prop.val.aircraft.iata", "aircraft_iata")\
                                .withColumnRenamed("prop.val.aircraft.icao", "aircraft_icao")\
                                .withColumnRenamed("prop.val.aircraft.icao24", "aircraft_icao24")\
                                .withColumnRenamed("prop.val.live.updated", "live_updated")\
                                .withColumnRenamed("prop.val.departure.delay", "departure_delay")\
                                .withColumnRenamed("prop.val.arrival.delay", "arrival_delay")\
                                .withColumnRenamed("prop.val.live.latitude", "live_latitude")\
                                .withColumnRenamed("prop.val.live.longitude", "live_longitude")\
                                .withColumnRenamed("prop.val.live.altitude.double", "live_altitude_double")\
                                .withColumnRenamed("prop.val.live.speed_horizontal", "live_speed_horizontal")\
                                .withColumnRenamed("prop.val.live.speed_vertical.int", "live_speed_vertical_int")\
                                .withColumnRenamed("prop.val.live.is_ground", "live_is_ground")\
                                .withColumnRenamed("prop.val.live.direction.double", "live_direction_double")\
                                .withColumnRenamed("prop.val.live.altitude.int", "live_altitude_int")\
                                .withColumnRenamed("prop.val.live.speed_vertical.double", "live_speed_vertical_double")\
                                .withColumnRenamed("prop.val.live.direction.int", "live_direction_int")



    
    df_flightdata.write.mode('append').parquet(s3_target+'/'+dt)
    
    



# Drop current partition

# spark.sql("use " + s3_dbname)

# old_partition = "SHOW PARTITIONS " + target_file
# old_partition_nm = old_partition[-19:]
# drop_old_partition = "ALTER TABLE " + s3_dbname + "." + target_file + " DROP PARTITION (partition_0= '" + old_partition_nm + "\')"
# partDF = spark.sql(drop_old_partition)

# # Point Glue catalog to the latest partition
# partition_location = s3_target + '/' + dt
# table_partition_ddl = "ALTER TABLE " + s3_dbname + "." + target_file + " ADD PARTITION" "(partition_0 = \'" + dt + "\') location \'" + partition_location + "/\' "
# partDF = spark.sql(table_partition_ddl)
    
job.commit()