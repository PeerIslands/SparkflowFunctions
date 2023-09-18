package io.peerislands.sparkflow.common

import org.apache.spark.sql.SparkSession

trait SparkSupport {
  lazy val sparkSession: SparkSession =
    SparkSession.builder
      .master("local[*]")
      .appName("Sparkflow functions test")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
      .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
      .config("spark.sql.extensions","org.apache.spark.sql.extra.NameCompareExtensions")
      .getOrCreate()
}

