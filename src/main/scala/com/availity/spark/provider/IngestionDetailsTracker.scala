package com.availity.spark.provider

import scala.collection.mutable.ArrayBuffer
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class IngestionDetailsTracker {
  var ingested_file_details = ArrayBuffer[Map[String, String]]()

  def record_file_details(path: String, movement: String, ts_format: String = "yyyy-MM-dd hh:mm:ss"): Unit = {
    val current_ts: LocalDateTime = LocalDateTime.now()
    val ts_formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(ts_format)
    val formatted_ts: String = current_ts.format(ts_formatter)
    this.ingested_file_details.append(Map("path" -> s"$path", "type" -> s"$movement", "ingestion_ts" -> s"$formatted_ts"))
  }

  def print_file_details(): Unit = {
    println("Ingested File Details")
    if (ingested_file_details.isEmpty){
      println("Warning: No files were ingested")
    }
    else for (file_detail <- ingested_file_details) {
      println(s"""\"Path\": ${file_detail.getOrElse("path", "")}, \"Type\": ${file_detail.getOrElse("Type", "")}, \"Ingestion Timestamp\": ${file_detail.getOrElse("ingestion_ts", "")}""")
    }
  }

}
