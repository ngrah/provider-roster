package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, lit}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import scala.reflect.io.Path


class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {
  val config_path = "config/config.json"

  val file_configs = ConfigUtils.read_configs(config_path)
  val file_1_config = file_configs._1
  val file_2_config = file_configs._2
  val visits_per_provider_config = file_configs._3
  val monthly_visits_per_provider_config = file_configs._4

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Provider-Roster")
    .getOrCreate()
  println("spark session created")
  spark.sparkContext.setLogLevel("WARN")

  val file_detail_tracker = new IngestionDetailsTracker()

  //  override def beforeEach(): Unit = {
  //  }

  //  override def afterEach(): Unit = {
  //  }

  describe("ProviderRoster") {
    describe("file 1 config verification") {
      val header = file_1_config.getOrElse("header", "")
      val delimiter = file_1_config.getOrElse("delimiter", "")
      val schema = file_1_config.getOrElse("schema", "")
      val file_path = file_1_config.getOrElse("path", "")
      val file_type = file_1_config.getOrElse("file_type", "csv")
      it("header config value is not y/n/empty") {
        assert(header == "y" || header == "n" || header == "")
      }
      it("delimiter config value is not provided") {
        assert(delimiter != "")
      }
      it("schema config value is not a valid schema") {
        assert(schema == "visits" || schema == "")
      }
      it("file_path config value is not a valid file path") {
        val fp = Path(file_path)
        assert(fp.exists)
        assert(fp.isFile)
        assert(fp.canRead)
        assert(!fp.isEmpty)
      }
      it("file_type config value is not a valid file type") {
        assert(file_type == "csv")
      }
    }

    describe("file 2 config verification") {
      val header = file_2_config.getOrElse("header", "")
      val delimiter = file_2_config.getOrElse("delimiter", "")
      val schema = file_2_config.getOrElse("schema", "")
      val file_path = file_2_config.getOrElse("path", "")
      val file_type = file_2_config.getOrElse("file_type", "csv")
      it("header config value is not y/n/empty") {
        assert(header == "y" || header == "n" || header == "")
      }
      it("delimiter config value is not provided") {
        assert(delimiter != "")
      }
      it("schema config value is not a valid schema") {
        assert(schema == "visits" || schema == "")
      }
      it("file_path config value is not a valid file path") {
        val fp = Path(file_path)
        assert(fp.exists)
        assert(fp.isFile)
        assert(fp.canRead)
        assert(!fp.isEmpty)
      }
      it("file_type config value is not a valid file type") {
        assert(file_type == "csv")
      }
    }

    val providers_df = IngestionUtils.ingest_file(spark, file_1_config, file_detail_tracker)
    describe("providers_df ingestion verification") {
      it("verify providers_df") {
        assert(!providers_df.isEmpty)
        assert(providers_df.schema.fields.size == 5)
      }
    }

    val visits_df = IngestionUtils.ingest_file(spark, file_2_config, file_detail_tracker)
    describe("visits_df ingestion verification") {
      it("verify visits_df") {
        assert(!visits_df.isEmpty)
        assert(visits_df.schema.fields.size == 3)
      }
    }

    val visits_providers_df = visits_df.join(providers_df, "provider_id")
    describe("join verification") {
      it("verify joined visits_providers_df") {
        assert(visits_providers_df.count() != 0)
      }
    }

    val visits_per_provider_df = visits_providers_df.
      selectExpr("provider_id", "concat(first_name,' ',middle_name,' ',last_name) as provider_name", "provider_specialty", "visit_id").
      groupBy("provider_id", "provider_name", "provider_specialty").agg(count("visit_id").alias("number_of_visits"))
    ExportUtils.export_json(visits_per_provider_df, visits_per_provider_config)
    describe("visits_per_provider verification") {
      it("dataframe verification") {
        assert(visits_per_provider_df.schema.fields.size == 4)
        assert(visits_per_provider_df.count() == providers_df.count())
      }
      it("files verification") {
        val fp = Path(visits_per_provider_config.getOrElse("path", ""))
        assert(fp.exists)
        assert(fp.isDirectory)
        assert(!fp.isEmpty)
      }
    }

    val monthly_visits_per_provider_df = visits_providers_df.
      selectExpr("provider_id", "from_unixtime(unix_timestamp(visit_service_date, 'yyyy-MM-dd'),'MMMM') as month", "visit_id").
      groupBy("provider_id", "month").agg(count("visit_id").alias("number_of_visits"))
    ExportUtils.export_json(monthly_visits_per_provider_df, monthly_visits_per_provider_config)
    describe("monthly_visits_per_provider verification") {
      it("dataframe verification") {
        assert(monthly_visits_per_provider_df.schema.fields.size == 3)
        assert(monthly_visits_per_provider_df.count() == visits_providers_df.
          selectExpr("provider_id", "from_unixtime(unix_timestamp(visit_service_date, 'yyyy-MM-dd'),'MMMM') as month").distinct().count())
      }
      it("files verification") {
        val fp = Path(monthly_visits_per_provider_config.getOrElse("path", ""))
        assert(fp.exists)
        assert(fp.isDirectory)
        assert(!fp.isEmpty)
      }
    }
  }

}
