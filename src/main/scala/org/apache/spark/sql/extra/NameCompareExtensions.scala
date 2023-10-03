package org.apache.spark.sql.extra

import org.apache.spark.name_compare.Extensions
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.namecompare.{NameCompare, NameCompare_2, NameCompare_3}


class NameCompareExtensions extends Extensions {
  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectFunction(NameCompare.fd)
    ext.injectFunction(NameCompare_2.fd)
    ext.injectFunction(NameCompare_3.fd)
  }
}

