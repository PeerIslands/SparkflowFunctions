package io.peerislands.sparkflow
import io.peerislands.sparkflow.common.SparkSupport
import org.apache.spark.sql.functions._
import org.apache.spark.sql.pi_compare._

object main extends App with SparkSupport {



  import sparkSession.implicits._
  val df = Seq("1").toDF("num")
  val name1 = struct(lit("john").as("fname"),lit("M").as("mname"),lit("Doe").as("lname"))
  val name2 = struct(lit("john").as("fname"),lit("M").as("mname"),lit("Doe").as("lname"))

  val newDf = df.withColumn("name1",name1).withColumn("name2",name2).withColumn("flag",compare_name(name1,name2))
    .withColumn("flag_2",compare_name_2(to_json(name1),to_json(name2)))
    .withColumn("flag_expr",expr("pi_compare_name(name1,name2)"))
    .withColumn("flag_expr_2",expr("pi_compare_name_2(to_json(name1),to_json(name2))"))
  newDf.show(10,false)

}
