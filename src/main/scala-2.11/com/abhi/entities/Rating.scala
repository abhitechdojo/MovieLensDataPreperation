package com.abhi.entities

import org.joda.time.{DateTimeZone, DateTime}

/**
  * Created by abhishek.srivastava on 2/28/16.
  */
case class Rating(userId: Int, movieId: Int, rating: Int, ratingTimestamp : DateTime)

object Rating {
  val baseTime : DateTime = new DateTime(1970, 1, 1, 0, 0, DateTimeZone.UTC)
  def apply(s: String) : Rating = {
    val Array(uId, mId, r, t) = s.split("::").map(_.toInt)
    new Rating(uId, mId, r, baseTime.plusSeconds(t))
  }
}
