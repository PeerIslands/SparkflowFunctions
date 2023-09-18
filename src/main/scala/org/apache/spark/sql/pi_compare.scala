package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.namecompare.{NameCompare,NameCompare_2}
import org.apache.spark.sql.catalyst.expressions.{Expression, StructsToJson}

object pi_compare {
  private def withExpr(expr: Expression): Column = Column(expr)



  /**
   *
   */
  def compare_name(left: Column, right : Column): Column = {
    withExpr {
      NameCompare(left.expr,right.expr)
    }

  }

  def compare_name_2(left: Column, right: Column): Column = {
    withExpr {
      NameCompare_2(left.expr, right.expr)
    }

  }

}
