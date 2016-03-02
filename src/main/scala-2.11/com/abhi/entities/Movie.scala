package com.abhi.entities

/**
  * Created by abhishek.srivastava on 2/28/16.
  */
case class Movie(movieId: Int, title: String, year: Int, genre: Set[String])

object Movie {
  val regex = "^(.+)\\s*\\((\\d+)\\)$".r
  def apply(s: String) : Movie = {
    val Array(id, titleStr, genreStr) = s.split("::")
    val regex(title, year) = titleStr
    val genreList1 = genreStr.split("\\|").toList
    val genreList = genreList1.toSet
    new Movie(id.toInt, title, year.toInt, genreList)
  }
}