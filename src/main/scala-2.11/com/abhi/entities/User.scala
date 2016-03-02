package com.abhi.entities

import scala.collection.immutable.HashMap

/**
  * Created by abhishek.srivastava on 2/28/16.
  */
case class User(id: Int, gender: String, age: Int, occupation: String, zipCode: String)

object User {

  val occupationMap = HashMap(
    0 -> "other",
    1 -> "academic/educator",
    2 -> "artist",
    3 -> "clerical/admin",
    4 -> "college/grad student",
    5 -> "customer service",
    6 -> "doctor/health care",
    7 -> "executive/managerial",
    8 -> "farmer",
    9 -> "homemaker",
    10 -> "K-12 student",
    11 -> "lawyer",
    12 -> "programmer",
    13 -> "retired",
    14 -> "sales/marketing",
    15 -> "scientist",
    16 -> "self-employed",
    17 -> "technician/engineer",
    18 -> "tradesman/craftsman",
    19 -> "unemployed",
    20 -> "writer"
  )

  def apply(s: String) = {
    val Array(id, gender, age, occupation, zipCode) = s.split("::")
    new User(id.toInt, gender, age.toInt, occupationMap(occupation.toInt), zipCode)
  }
}