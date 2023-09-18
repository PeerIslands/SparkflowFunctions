package org.apache.spark.sql.catalyst.expressions.namecompare

import io.peerislands.sparkflow.demographics.{Name, NameComparator}
import org.apache.spark.name_compare.FunctionDescription
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ExpressionInfo}
import org.apache.spark.sql.types._

case class NameCompare(left : Expression, right : Expression)
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


  @transient
  private lazy val leftSchema: StructType = {
      left.dataType.asInstanceOf[StructType]
  }



  @transient
  private lazy val rightSchema: StructType = {
    right.dataType.asInstanceOf[StructType]
  }



  override def nullSafeEval(left: Any, right: Any): Any = {
    (left,right) match {
      case (leftNameRow: InternalRow, rightNameRow: InternalRow ) =>
        val leftName = ExpressionEncoder[Name]().resolveAndBind(leftSchema.toAttributes).objDeserializer.eval(input = leftNameRow).asInstanceOf[Name]
        val rightName = ExpressionEncoder[Name]().resolveAndBind(rightSchema.toAttributes).objDeserializer.eval(input = rightNameRow).asInstanceOf[Name]
        val flag = NameComparator(leftName, rightName).compare()
        InternalRow(flag.value().toBinaryString, flag.value())
      case _ => InternalRow(Long.MinValue.toBinaryString, Long.MinValue)
    }

  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, StructType)
}

object NameCompare {
  val fd: FunctionDescription = (
    new FunctionIdentifier("pi_compare_name"),
    new ExpressionInfo(classOf[NameCompare].getCanonicalName, "pi_compare_name"),
    (children: Seq[Expression]) => NameCompare(children.head, children.last))
}
