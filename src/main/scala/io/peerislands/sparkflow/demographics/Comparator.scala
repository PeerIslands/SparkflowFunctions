package io.peerislands.sparkflow.demographics

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods


case class NameComparator(leftName: Name, rightName: Name){
   def compare(): MatchInformation = {
    var flag = if(rightName.fname == leftName.fname) {
      new MatchInformation() | MatchFlag.fnameMatch
    } else {
      new MatchInformation()
    }
    flag = if (rightName.lname == leftName.lname) {
      flag | MatchFlag.lnameMatch
    } else {
      flag
    }

    flag = if (rightName.mname == leftName.mname) {
      flag | MatchFlag.mnameMatch
    } else {
      flag
    }
    flag
  }
}

object NameCompareHelper {
  def apply(left: UTF8String, right : UTF8String) : InternalRow = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val name1 = JsonMethods.parse(left.toString).extract[Name]
    val name2 = JsonMethods.parse(right.toString).extract[Name]
    val flag = NameComparator(name1, name2).compare()
    InternalRow(UTF8String.fromString(flag.value().toBinaryString), flag.value())
  }
}