package com.availity.spark.provider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, DateType, StringType}
import org.apache.spark.sql.functions.{count, lit, array, collect_list, col, month, avg}

object ProviderRoster {

  def main(args: Array[String]): Unit = {

    val config_path = args(0)
    println(config_path)
    val file_configs = ConfigUtils.read_configs(config_path)
    val file_1_config = file_configs._1
    val file_2_config = file_configs._2
    val visits_per_provider_config = file_configs._3
    val monthly_visits_per_provider_config = file_configs._4
    val file_detail_tracker = new IngestionDetailsTracker()

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Provider-Roster")
      .getOrCreate()
    println("spark session created")
    spark.sparkContext.setLogLevel("WARN")

    val providers_df = IngestionUtils.ingest_file(spark, file_1_config, file_detail_tracker)
    val visits_df = IngestionUtils.ingest_file(spark, file_2_config, file_detail_tracker)
    file_detail_tracker.print_file_details()

    val visits_providers_df = visits_df.join(providers_df, "provider_id")

    /*
    Given the two data datasets, calculate the total number of visits per provider.
    The resulting set should contain the provider's ID, name, specialty, along with the number of visits.
    Output the report in json, partitioned by the provider's specialty.
    */
    val visits_per_provider_df = visits_providers_df.
      selectExpr("provider_id", "concat(first_name,' ',middle_name,' ',last_name) as provider_name", "provider_specialty", "visit_id").
      groupBy("provider_id", "provider_name", "provider_specialty").agg(count("visit_id").alias("number_of_visits"))
    visits_per_provider_df.show
    ExportUtils.export_json(visits_per_provider_df, visits_per_provider_config)

    /*
    Given the two datasets, calculate the total number of visits per provider per month.
    The resulting set should contain the provider's ID, the month, and total number of visits.
    Output the result set in json.
    */
    val monthly_visits_per_provider_df = visits_providers_df.
      selectExpr("provider_id", "from_unixtime(unix_timestamp(visit_service_date, 'yyyy-MM-dd'),'MMMM') as month", "visit_id").
      groupBy("provider_id", "month").agg(count("visit_id").alias("number_of_visits"))
    monthly_visits_per_provider_df.show
    ExportUtils.export_json(monthly_visits_per_provider_df, monthly_visits_per_provider_config)

  }
}
