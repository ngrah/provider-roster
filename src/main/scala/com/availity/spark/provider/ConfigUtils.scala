package com.availity.spark.provider

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import java.io.File
import scala.io.Source

object ConfigUtils {
  implicit val formats = DefaultFormats

  def yn_to_boolean(yn: String): Boolean = {
    val tf: Boolean = yn match {
      case "y" => {
        true
      }
      case "n" => {
        false
      }
      case _ => false
    }
    tf
  }

  def read_configs(json_path: String): (Map[String, String], Map[String, String], Map[String, String], Map[String, String]) = {
    println("parsing config.json")
    val json_string = Source.fromFile(new File(json_path)).getLines().mkString("")
    println(json_string)
    val config = parse(json_string).extract[root_interface_]
    val file_config_1 = extract_file_configs(config.config.src_file_1)
    val file_config_2 = extract_file_configs(config.config.src_file_2)
    val export_config_1 = extract_export_file_configs(config.config.visits_per_provider_file)
    val export_config_2 = extract_export_file_configs(config.config.monthly_visits_per_provider_file)
    (file_config_1, file_config_2, export_config_1, export_config_2)
  }

  def extract_file_configs(file_dtl: file_details_): Map[String, String] = {
    val detail_map = Map(
      "path" -> file_dtl.path.toString,
      "header" -> file_dtl.header.toString.toLowerCase,
      "schema" -> file_dtl.schema.toString.toLowerCase,
      "delimiter" -> file_dtl.delimiter.toString,
      "file_type" -> file_dtl.file_type.toString,
    )
    detail_map
  }

  def extract_export_file_configs(op_file_dtl: export_file_details_): Map[String, String] = {
    val detail_map = Map(
      "path" -> op_file_dtl.path.toString,
      "format" -> op_file_dtl.format.toString.toLowerCase,
      "partition" -> op_file_dtl.partition.toString
    )
    detail_map
  }

  case class root_interface_(
                              config: config_
                            )

  case class config_(
                      src_file_1: file_details_,
                      src_file_2: file_details_,
                      visits_per_provider_file: export_file_details_,
                      monthly_visits_per_provider_file: export_file_details_
                    )

  case class file_details_(
                            path: String,
                            delimiter: String,
                            schema: String,
                            header: String,
                            file_type: String
                          )

  case class export_file_details_(
                                   path: String,
                                   format: String,
                                   partition: String
                                 )
}
