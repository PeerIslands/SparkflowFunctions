package org.apache.spark.sql.catalyst.expressions.namecompare

import io.peerislands.sparkflow.demographics.NameCompareHelper
import org.apache.spark.name_compare.FunctionDescription
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionInfo}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class NameCompare_3(left : Expression, right : Expression)
  extends BinaryExpression with ExpectsInputTypes {
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
    NameCompareHelper(left.asInstanceOf[UTF8String],right.asInstanceOf[UTF8String])
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (l, r) => {
        s"""
           |${ev.value} =
           |io.peerislands.sparkflow.demographics.NameCompareHelper.apply($l,$r));
       """.stripMargin
      }
    )
  }
}

object NameCompare_3 {
  val fd: FunctionDescription = (
    new FunctionIdentifier("pi_compare_name_3"),
    new ExpressionInfo(classOf[NameCompare].getCanonicalName, "pi_compare_name_3"),
    (children: Seq[Expression]) => NameCompare_3(children.head, children.last))
}