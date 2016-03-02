package com.abhi

import java.nio.charset.CodingErrorAction

import com.datastax.driver.core.Cluster
import com.websudos.phantom.connectors.{ContactPoints}

import scala.collection.immutable.HashMap
import org.joda.time.{DateTimeZone, DateTime}
import scala.io.{Codec, Source}
import java.io.PrintWriter
import java.io.File
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.websudos.phantom.connectors.{KeySpace, SessionProvider}
import com.abhi.entities._
import com.abhi.models._

/**
  * Created by abhishek.srivastava on 1/22/16.
  */
object MovieLensDataPreperation {

  def main(args: Array[String]) : Unit = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val inputPath = "/Users/abhishek.srivastava/Downloads/ml-1m"

    val userList : List[User] = Source
      .fromFile(s"$inputPath/users.dat")
      .getLines
      .map(line => User(line))
      .toList

    val ratingList : List[Rating] = Source
      .fromFile(s"$inputPath/ratings.dat")
      .getLines
      .map(line => Rating(line))
      .toList

    val movieList : List[Movie] = Source
      .fromFile(s"$inputPath/movies.dat")
      .getLines
      .map(line => Movie(line))
      .toList

    //toFirebaseJson(inputPath, userList, ratingList, movieList)
    storeInCassandra(userList, ratingList, movieList)
  }

  def storeInCassandra(userList: List[User], ratingList: List[Rating], movieList: List[Movie]) = {
    movieList.foreach(m => Movies.store(m))
    userList.foreach(u => Users.store(u))
    ratingList.foreach(r => Ratings.store(r))
  }

  def toFirebaseJson(inputPath: String, userList : List[User], ratingList : List[Rating], movieList : List[Movie]) = {

    val usersJsonTmpl = (
      "users" -> userList.map { u => (
          ("id" -> u.id) ~
          ("gender" -> u.gender) ~
          ("age" -> u.age) ~
          ("occupation" -> u.occupation) ~
          ("zipCode" -> u.zipCode)
        )
      }
    )

    // 2. read the ratings file

    val ratingsJsonTmpl = (
      "ratings" -> ratingList.map{r => (
          ("movieid" -> r.movieId) ~
          ("userId" -> r.userId) ~
          ("rating" -> r.rating) ~
          ("ratingTimestamp" -> r.ratingTimestamp.toString)
        )}
    )

    val movieJsonTmpl = (
      "movies" -> movieList.map{m => (
        ("id" -> m.movieId) ~
        ("title" -> m.title) ~
        ("year" -> m.year.toString) ~
        ("genre" -> m.genre.toList)
      )}
    )

    val pw = new PrintWriter(new File(s"$inputPath/firebase.json" ))
    //val moviesData : String = "\"movies\": " + movieList.mkString("{", ",", "}")

    val movieLensTmpl = (
      "movieLens" ->
        movieJsonTmpl ~
        usersJsonTmpl ~
        ratingsJsonTmpl
      )

    val movieLensJson = compact(render(movieLensTmpl))

    pw.write(movieLensJson)
    pw.close

  }
}