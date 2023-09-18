package io.peerislands.sparkflow.demographics


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