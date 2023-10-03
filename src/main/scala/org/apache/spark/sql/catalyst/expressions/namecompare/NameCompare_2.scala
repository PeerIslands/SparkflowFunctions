package org.apache.spark.sql.catalyst.expressions.namecompare

import io.peerislands.sparkflow.demographics.{Name, NameComparator}
import org.apache.spark.name_compare.FunctionDescription
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionInfo}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

case class NameCompare_2(left : Expression, right : Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {
  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }



  override def dataType: DataType = StructType(
    Seq(
      StructField("output", StringType),
      StructField("value", LongType)
    )
  )

  override def nullSafeEval(left: Any, right: Any): Any = {
    (left,right) match {
      case (leftJson: UTF8String, rightJson: UTF8String ) =>
        implicit val formats: DefaultFormats.type = DefaultFormats
          val name1 = JsonMethods.parse(leftJson.toString).extract[Name]
          val name2 = JsonMethods.parse(rightJson.toString).extract[Name]
         val flag = NameComparator(name1,name2).compare()
        InternalRow(UTF8String.fromString(flag.value().toBinaryString), flag.value())
      case _ => InternalRow(Long.MinValue.toBinaryString, Long.MinValue)
    }

  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

    }

object NameCompare_2 {
  val fd: FunctionDescription = (
    new FunctionIdentifier("pi_compare_name_2"),
    new ExpressionInfo(classOf[NameCompare].getCanonicalName, "pi_compare_name_2"),
    (children: Seq[Expression]) => NameCompare_2(children.head, children.last))
}