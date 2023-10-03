package io.peerislands.sparkflow
import io.peerislands.sparkflow.common.SparkSupport
import org.apache.spark.sql.functions._
import org.apache.spark.sql.pi_compare._



object main extends App with SparkSupport {

  val df = sparkSession.read.format("parquet").load("/Users/ganeshparasuraman/Documents/nc_voter")
    .withColumn("name1",struct(lit("givenname").as("fname"),lit("M").as("mname"),lit("surname").as("lname")))
    .withColumn("name2",struct(lit("givenname").as("fname"),lit("M").as("mname"),lit("surname").as("lname")))
    .select("recid","name1","name2")


  val newDf = df
   //.withColumn("flag",compare_name(col("name1"),col("name2")))
   //.withColumn("flag_2",compare_name_2(to_json(col("name1")),to_json(col("name2"))))
        // .withColumn("flag_3",compare_name_3(to_json(col("name1")),to_json(col("name2"))))
    .withColumn("flag_expr",expr("pi_compare_name(name1,name2)"))
//    .withColumn("flag_expr_2",expr("pi_compare_name_2(to_json(name1),to_json(name2))"))
//    .withColumn("flag_expr_3",expr("pi_compare_name_3(to_json(name1),to_json(name2))"))
  sparkSession.time{
    newDf.write.format("noop").mode("overwrite").save()
  }


}
