package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExportUtils {
  def export_json(df: DataFrame, export_file_configs: Map[String, String]): Unit = {
    val file_writer = df.coalesce(1).write
    val partition = export_file_configs.getOrElse("partition", "")
    if (partition.isEmpty) {
      println("Partition config value is not provided")
    } else {
      file_writer.partitionBy(partition)
    }

    val file_path = export_file_configs.getOrElse("path", "")
    if (file_path.isEmpty) {
      println("File path config is required")
    }

    val file_format = export_file_configs.getOrElse("format", "json").toLowerCase
    file_format match {
      case "json" => {
        file_writer.mode("Overwrite").json(file_path)
      }
      case _ => {
        println("Provided file format is not supported")
      }
    }
  }
}
