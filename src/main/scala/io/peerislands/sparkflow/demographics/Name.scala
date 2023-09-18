package io.peerislands.sparkflow.demographics

import io.peerislands.sparkflow.demographics.MatchFlag.MatchFlags


case class Name(fname : Option[String], lname: Option[String], mname: Option[String])


object MatchFlag {
  type MatchFlags = Long;
  val none: MatchFlags = 0L
  val fnameMatch: MatchFlags = 1L << 0
  val mnameMatch: MatchFlags = 1L << 1
  val lnameMatch: MatchFlags = 1L << 2
  val fnamePartialMatch: MatchFlags = 1L  << 3
  val mnamePartialMatch: MatchFlags = 1L  << 4
  val lnamePartialMatch: MatchFlags = 1L  << 5
}

class MatchInformation(flag : MatchFlags = MatchFlag.none) {
  def |(matchFlags: MatchFlags): MatchInformation = {
    new MatchInformation(flag | matchFlags )
  }

  def |(right: MatchFlags, left: MatchFlags): MatchInformation = {
    new MatchInformation(right | left)
  }

  def value(): MatchFlags = {
    this.flag
  }
}



