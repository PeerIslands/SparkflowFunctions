package org.apache.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.EmptyFunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

package object name_compare {
  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
  type Extensions = SparkSessionExtensions => Unit
}
