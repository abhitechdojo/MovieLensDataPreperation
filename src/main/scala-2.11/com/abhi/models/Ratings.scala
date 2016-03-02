package com.abhi.models

import com.abhi.connector.CassandraConnector
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import org.joda.time.DateTime
import scala.concurrent.Future
import com.abhi.entities.Rating

class Ratings extends CassandraTable[Ratings, Rating] {
  object userId extends IntColumn(this) with PartitionKey[Int]
  object movieId extends IntColumn(this) with PartitionKey[Int]
  object rating extends IntColumn(this) with Index[Int]
  object ratingTimestamp extends DateTimeColumn(this) with Index[DateTime]

  def fromRow(row: Row) : Rating = {
    Rating(
      userId(row),
      movieId(row),
      rating(row),
      ratingTimestamp(row)
    )
  }
}

object Ratings extends Ratings with CassandraConnector {
  def store(rating: Rating) : Future[ResultSet] = {
    insert
      .value(_.userId, rating.userId)
      .value(_.movieId, rating.movieId)
      .value(_.rating, rating.rating)
      .value(_.ratingTimestamp, rating.ratingTimestamp)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def getById(userId: Int, movieId: Int) : Future[Option[Rating]] = {
    select
      .where(_.userId eqs userId).and(_.movieId eqs movieId)
      .one()
  }
}