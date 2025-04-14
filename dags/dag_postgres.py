from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args ={
    'owner': 'tommy',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    start_date=datetime(2025, 4, 10),
    schedule_interval='0 0 * * *'
) as dag:
    task1=PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE "dim_time" (
            "time_id" SERIAL PRIMARY KEY,
            "time_stamp" timestamp,
            "year" int,
            "month" int,
            "day" int,
            "hour" int,
            "minute" int
            );

            CREATE TABLE "dim_ambient_conditions" (
            "ambient_id" SERIAL PRIMARY KEY,
            "ambient_humidity" double precision,
            "ambient_temperature" double precision
            );

            CREATE TABLE "dim_machine" (
            "machine_id" SERIAL PRIMARY KEY,
            "machine_name" varchar,
            "machine_type" varchar,
            "stage" int,
            "last_maintenance_date" date
            );

            CREATE TABLE "dim_machine1_motor" (
            "machine1_motor_id" SERIAL PRIMARY KEY,
            "motor_amperage" double precision,
            "motor_rpm" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_machine2_motor" (
            "machine2_motor_id" SERIAL PRIMARY KEY,
            "motor_amperage" double precision,
            "motor_rpm" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_machine3_motor" (
            "machine3_motor_id" SERIAL PRIMARY KEY,
            "motor_amperage" double precision,
            "motor_rpm" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_machine1_zone_temperature" (
            "machine1_zone_temp_id" SERIAL PRIMARY KEY,
            "zone1_temperature" double precision,
            "zone2_temperature" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_machine2_zone_temperature" (
            "machine2_zone_temp_id" SERIAL PRIMARY KEY,
            "zone1_temperature" double precision,
            "zone2_temperature" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_machine3_zone_temperature" (
            "machine3_zone_temp_id" SERIAL PRIMARY KEY,
            "zone1_temperature" double precision,
            "zone2_temperature" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_machine1_material_properties" (
            "machine1_material_id" SERIAL PRIMARY KEY,
            "raw_material_property1" double precision,
            "raw_material_property2" int,
            "raw_material_property3" double precision,
            "raw_material_property4" int,
            "machine_id" int
            );

            CREATE TABLE "dim_machine2_material_properties" (
            "machine2_material_id" SERIAL PRIMARY KEY,
            "raw_material_property1" double precision,
            "raw_material_property2" int,
            "raw_material_property3" double precision,
            "raw_material_property4" int,
            "machine_id" int
            );

            CREATE TABLE "dim_machine3_material_properties" (
            "machine3_material_id" SERIAL PRIMARY KEY,
            "raw_material_property1" double precision,
            "raw_material_property2" int,
            "raw_material_property3" double precision,
            "raw_material_property4" int,
            "machine_id" int
            );

            CREATE TABLE "dim_combiner_temperature" (
            "combiner_temp_id" SERIAL PRIMARY KEY,
            "temperature1" double precision,
            "temperature2" double precision,
            "temperature3" double precision
            );

            CREATE TABLE "dim_first_stage_actual" (
            "first_stage_actual_id" SERIAL PRIMARY KEY,
            "measurement0_actual" double precision,
            "measurement1_actual" double precision,
            "measurement2_actual" double precision,
            "measurement3_actual" double precision,
            "measurement4_actual" double precision,
            "measurement5_actual" double precision,
            "measurement6_actual" double precision,
            "measurement7_actual" double precision,
            "measurement8_actual" double precision,
            "measurement9_actual" double precision,
            "measurement10_actual" double precision,
            "measurement11_actual" double precision,
            "measurement12_actual" double precision,
            "measurement13_actual" double precision,
            "measurement14_actual" double precision
            );

            CREATE TABLE "dim_first_stage_setpoint" (
            "first_stage_setpoint_id" SERIAL PRIMARY KEY,
            "measurement0_setpoint" double precision,
            "measurement1_setpoint" double precision,
            "measurement2_setpoint" double precision,
            "measurement3_setpoint" double precision,
            "measurement4_setpoint" double precision,
            "measurement5_setpoint" double precision,
            "measurement6_setpoint" double precision,
            "measurement7_setpoint" double precision,
            "measurement8_setpoint" double precision,
            "measurement9_setpoint" double precision,
            "measurement10_setpoint" double precision,
            "measurement11_setpoint" double precision,
            "measurement12_setpoint" double precision,
            "measurement13_setpoint" double precision,
            "measurement14_setpoint" double precision
            );

            CREATE TABLE "dim_machine4_temperature_pressure" (
            "machine4_temp_pressure_id" SERIAL PRIMARY KEY,
            "temperature1" double precision,
            "temperature2" double precision,
            "pressure" double precision,
            "temperature3" double precision,
            "temperature4" double precision,
            "temperature5" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_machine5_temperature" (
            "machine5_temp_id" SERIAL PRIMARY KEY,
            "temperature1" double precision,
            "temperature2" double precision,
            "temperature3" double precision,
            "temperature4" double precision,
            "temperature5" double precision,
            "temperature6" double precision,
            "machine_id" int
            );

            CREATE TABLE "dim_exit_temperature" (
            "exit_temp_id" SERIAL PRIMARY KEY,
            "machine4_exit_temperature" double precision,
            "machine5_exit_temperature" double precision
            );

            CREATE TABLE "dim_second_stage_actual" (
            "second_stage_actual_id" SERIAL PRIMARY KEY,
            "measurement0_actual" double precision,
            "measurement1_actual" double precision,
            "measurement2_actual" double precision,
            "measurement3_actual" double precision,
            "measurement4_actual" double precision,
            "measurement5_actual" double precision,
            "measurement6_actual" double precision,
            "measurement7_actual" double precision,
            "measurement8_actual" double precision,
            "measurement9_actual" double precision,
            "measurement10_actual" double precision,
            "measurement11_actual" double precision,
            "measurement12_actual" double precision,
            "measurement13_actual" double precision,
            "measurement14_actual" double precision
            );

            CREATE TABLE "dim_second_stage_setpoint" (
            "second_stage_setpoint_id" SERIAL PRIMARY KEY,
            "measurement0_setpoint" double precision,
            "measurement1_setpoint" double precision,
            "measurement2_setpoint" double precision,
            "measurement3_setpoint" double precision,
            "measurement4_setpoint" double precision,
            "measurement5_setpoint" double precision,
            "measurement6_setpoint" double precision,
            "measurement7_setpoint" double precision,
            "measurement8_setpoint" double precision,
            "measurement9_setpoint" double precision,
            "measurement10_setpoint" double precision,
            "measurement11_setpoint" double precision,
            "measurement12_setpoint" double precision,
            "measurement13_setpoint" double precision,
            "measurement14_setpoint" double precision
            );

            CREATE TABLE "fact_stage1_operation" (
            "time_id" int,
            "ambient_id" int,
            "machine1_motor_id" int,
            "machine2_motor_id" int,
            "machine3_motor_id" int,
            "machine1_zone_temp_id" int,
            "machine2_zone_temp_id" int,
            "machine3_zone_temp_id" int,
            "machine1_material_id" int,
            "machine2_material_id" int,
            "machine3_material_id" int,
            "combiner_temp_id" int,
            "avg_motor_amperage" double precision,
            "avg_motor_rpm" double precision,
            "avg_material_pressure" double precision,
            "avg_material_temperature" double precision,
            "avg_exit_zone_temp" double precision
            );

            CREATE TABLE "fact_stage1_output" (
            "time_id" int,
            "ambient_id" int,
            "first_stage_actual_id" int,
            "first_stage_setpoint_id" int,
            "avg_measurement_actual" double precision,
            "avg_setpoint" double precision
            );

            CREATE TABLE "fact_stage2_operation" (
            "time_id" int,
            "ambient_id" int,
            "machine4_temp_pressure_id" int,
            "machine5_temp_id" int,
            "exit_temp_id" int,
            "avg_machine4_temperature" double precision,
            "avg_machine5_temperature" double precision,
            "avg_exit_temperature" double precision
            );

            CREATE TABLE "fact_stage2_output" (
            "time_id" int,
            "ambient_id" int,
            "second_stage_actual_id" int,
            "second_stage_setpoint_id" int,
            "avg_measurement_actual" double precision,
            "avg_setpoint" double precision
            );

            ALTER TABLE "dim_machine1_motor" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine2_motor" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine3_motor" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine1_zone_temperature" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine2_zone_temperature" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine3_zone_temperature" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine1_material_properties" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine2_material_properties" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine3_material_properties" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine4_temperature_pressure" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "dim_machine5_temperature" ADD FOREIGN KEY ("machine_id") REFERENCES "dim_machine" ("machine_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine1_motor_id") REFERENCES "dim_machine1_motor" ("machine1_motor_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine2_motor_id") REFERENCES "dim_machine2_motor" ("machine2_motor_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine3_motor_id") REFERENCES "dim_machine3_motor" ("machine3_motor_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine1_zone_temp_id") REFERENCES "dim_machine1_zone_temperature" ("machine1_zone_temp_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine2_zone_temp_id") REFERENCES "dim_machine2_zone_temperature" ("machine2_zone_temp_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine3_zone_temp_id") REFERENCES "dim_machine3_zone_temperature" ("machine3_zone_temp_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine1_material_id") REFERENCES "dim_machine1_material_properties" ("machine1_material_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine2_material_id") REFERENCES "dim_machine2_material_properties" ("machine2_material_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("machine3_material_id") REFERENCES "dim_machine3_material_properties" ("machine3_material_id");

            ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("combiner_temp_id") REFERENCES "dim_combiner_temperature" ("combiner_temp_id");

            ALTER TABLE "fact_stage1_output" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

            ALTER TABLE "fact_stage1_output" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");

            ALTER TABLE "fact_stage1_output" ADD FOREIGN KEY ("first_stage_actual_id") REFERENCES "dim_first_stage_actual" ("first_stage_actual_id");

            ALTER TABLE "fact_stage1_output" ADD FOREIGN KEY ("first_stage_setpoint_id") REFERENCES "dim_first_stage_setpoint" ("first_stage_setpoint_id");

            ALTER TABLE "fact_stage2_operation" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

            ALTER TABLE "fact_stage2_operation" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");

            ALTER TABLE "fact_stage2_operation" ADD FOREIGN KEY ("machine4_temp_pressure_id") REFERENCES "dim_machine4_temperature_pressure" ("machine4_temp_pressure_id");

            ALTER TABLE "fact_stage2_operation" ADD FOREIGN KEY ("machine5_temp_id") REFERENCES "dim_machine5_temperature" ("machine5_temp_id");

            ALTER TABLE "fact_stage2_operation" ADD FOREIGN KEY ("exit_temp_id") REFERENCES "dim_exit_temperature" ("exit_temp_id");

            ALTER TABLE "fact_stage2_output" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

            ALTER TABLE "fact_stage2_output" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");

            ALTER TABLE "fact_stage2_output" ADD FOREIGN KEY ("second_stage_actual_id") REFERENCES "dim_second_stage_actual" ("second_stage_actual_id");

            ALTER TABLE "fact_stage2_output" ADD FOREIGN KEY ("second_stage_setpoint_id") REFERENCES "dim_second_stage_setpoint" ("second_stage_setpoint_id");

        """
    )

    task1