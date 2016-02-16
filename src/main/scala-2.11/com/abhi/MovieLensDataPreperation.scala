package com.abhi

import scala.collection.immutable.HashMap
import org.joda.time.{DateTimeZone, DateTime}
import scala.io.Source
import java.io.PrintWriter
import java.io.File

import java.nio.charset.CodingErrorAction
import scala.io.Codec

/**
  * Created by abhishek.srivastava on 1/22/16.
  */
object MovieLensDataPreperation {

  def main(args: Array[String]) : Unit = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // 1. create users json file
    val userList : List[User] = Source
      .fromFile("/Users/abhishek.srivastava/Downloads/ml-1m/users.dat")
      .getLines
      .map(line => User(line))
      .toList

    // write to a json file as an array
    val usersData  = "\"users\": " + userList.mkString("{", ",", "}")

    // 2. read the ratings file
    val ratingList : List[(Rating, Int)] = Source
      .fromFile("/Users/abhishek.srivastava/Downloads/ml-1m/ratings.dat")
      .getLines
      .map(line => Rating(line))
      .zipWithIndex
      .toList

    // 2. read the movies file
    val movieList : List[Movie] = Source
      .fromFile("/Users/abhishek.srivastava/Downloads/ml-1m/movies.dat")
      .getLines
      .map(line => Movie(line))
      .toList

    val genreList = movieList.flatMap(movie => movie.genre).map(movie => "\"" + movie + "\"").distinct
    val yearList = movieList.map(movie => movie.year).distinct
    val genreListStr = "\"genre\": " + genreList.mkString("[", ",", "]");
    val yearListStr = "\"year\": " + yearList.mkString("[", ",", "]")
    val ratingData = "\"ratings\": " + ratingList.map({case(r, i) => s""""$i": ${r.toString}"""}).mkString("{", ",", "}")
    val pw = new PrintWriter(new File("/Users/abhishek.srivastava/Downloads/ml-1m/firebase.json" ))
    val moviesData : String = "\"movies\": " + movieList.mkString("{", ",", "}")

    val finalData = "{\"movieLens\":{" + usersData + ", " + moviesData + ", " + ratingData + ", " + genreListStr + ", " + yearListStr + "}}"
    pw.write(finalData)
    pw.close
  }
}

class User(val id: Int, val gender: String, val age: Int, val occupation: String, val zipCode: String) {
  override def toString() : String = {
    s"""
       |"${id}" : {
       |  "userid": ${id},
       |  "gender": "${gender}",
       |  "age": ${age},
       |  "occupation": "${occupation}",
       |  "zipCode": "${zipCode}"
       |}""".stripMargin
  }
}

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


class Rating(val userId: Int, val movieId: Int, val rating: Int, val timeStamp : DateTime) {
  override def toString() = {
    s"""{
       |  "userid" : ${userId},
       |  "movieid": ${movieId},
       |  "rating": ${rating},
       |  "timeStamp": "${timeStamp}"
       |}""".stripMargin
  }
}

object Rating {
  val baseTime : DateTime = new DateTime(1970, 1, 1, 0, 0, DateTimeZone.UTC)
  def apply(s: String) : Rating = {
    val Array(uId, mId, r, t) = s.split("::").map(_.toInt)
    new Rating(uId, mId, r, baseTime.plusSeconds(t))
  }
}

class Movie(val movieId: Int, val title: String, val year: Int, val genre: Array[String]) {
  override def toString() = {
    s"""
       |"${movieId}": {
       | "title": "${title}",
       | "year": ${year},
       | "genre": ${genre.map("\"" + _ + "\"").mkString("[", ",", "]")}
       |}""".stripMargin
  }
}

object Movie {
  val regex = "^(.+)\\s*\\((\\d+)\\)$".r
  def apply(s: String) = {
    val Array(id, titleStr, genreStr) = s.split("::")
    val regex(title, year) = titleStr
    val genreList = genreStr.split("\\|")
    new Movie(id.toInt, title, year.toInt, genreList)
  }
}