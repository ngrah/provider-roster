package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, lit}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  override def beforeEach: Unit = {
  }

  override def afterEach(): Unit = {
  }

  describe("process") {
    val config_path = "config/config.json"
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

    val providers_df = IngestionUtils.ingest_file(spark, file_1_config, file_detail_tracker)
    val visits_df = IngestionUtils.ingest_file(spark, file_2_config, file_detail_tracker)
    file_detail_tracker.print_file_details()
    val visits_providers_df = visits_df.join(providers_df, "provider_id")

    val visits_per_provider_df = visits_providers_df.
      selectExpr("provider_id", "concat(first_name,' ',middle_name,' ',last_name) as provider_name", "provider_specialty", "visit_id").
      groupBy("provider_id", "provider_name", "provider_specialty").agg(count("visit_id").alias("number_of_visits"))

    val monthly_visits_per_provider_df = visits_providers_df.
      selectExpr("provider_id", "from_unixtime(unix_timestamp(visit_service_date, 'yyyy-MM-dd'),'MMMM') as month", "visit_id").
      groupBy("provider_id", "month").agg(count("visit_id").alias("number_of_visits"))

    it("visits per provider column names") {
      assert(visits_per_provider_df.columns === Array("provider_id","provider_name","provider_specialty","number_of_visits"))
    }

    it("monthly visits per provider column names") {
      assert(monthly_visits_per_provider_df.columns === Array("provider_id", "month", "number_of_visits"))
    }

    it("visits per provider count") {
      val expected_count = providers_df.count()
        assert(visits_per_provider_df.count() === expected_count)
    }

    it("monthly visits per provider count") {
    val expected_count = visits_providers_df.
      selectExpr("provider_id", "from_unixtime(unix_timestamp(visit_service_date, 'yyyy-MM-dd'),'MMMM') as month").
      distinct().count()
      assert(monthly_visits_per_provider_df.count() === expected_count)
    }

  }
}
