package com.availity.spark.provider

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

//Utility functions for reading in the files
object IngestionUtils {

  def ingest_file(spark: SparkSession, file_configs: Map[String, String], file_details_tracker: IngestionDetailsTracker): DataFrame = {

    println("Ingesting a file, Configs: ")
    for (key <- file_configs.keys) {
      println(s"$key : " + file_configs.getOrElse(key, ""))
    }

    val file_reader = spark.read

    //get header config
    val header = file_configs.getOrElse("header", "")
    if (header.isEmpty || !(header == "y" || header == "n")) {
      println("Header config value is not provided")
    } else {
      file_reader.option("header", ConfigUtils.yn_to_boolean(header))
    }

    //get delimiter config
    val delimiter = file_configs.getOrElse("delimiter", "")
    if (delimiter.isEmpty) {
      println("delimiter config value is not provided")
    } else {
      file_reader.option("delimiter", delimiter)
    }

    val schema = file_configs.getOrElse("schema", "")
    if (schema.isEmpty) {
      println("Schema config value is not provided, inferring the schema")
      file_reader.option("inferSchema", true)
    } else {
      file_reader.option("inferSchema", false)
      if (schema == "visits") {
        file_reader.schema(visits_schema)
      }
    }

    val file_path = file_configs.getOrElse("path", "")
    if (file_path.isEmpty) {
      println("File path config is required")
    }

    val file_type = file_configs.getOrElse("file_type", "csv")
    var df: DataFrame = file_type match {
      case "csv" => {
        file_reader.csv(file_path)
      }

      case _ => {
        println("Invalid file_type provided")
        println(file_type)
        spark.createDataFrame(Seq())
      }
    }
    file_details_tracker.record_file_details(file_path, "Ingest")
    df
  }

  val visits_schema = StructType(Seq(StructField("visit_id", StringType, nullable = false), StructField("provider_id", StringType, nullable = false), StructField("visit_service_date", StringType, nullable = false)))


}
