package com.abhi.models

import com.abhi.connector.CassandraConnector
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import scala.concurrent.Future
import com.abhi.entities.Movie

class Movies extends CassandraTable[Movies, Movie] {
  object id extends IntColumn(this) with PartitionKey[Int]
  object title extends StringColumn(this) with Index[String]
  object year extends IntColumn(this) with Index[Int]
  object genre extends SetColumn[Movies, Movie, String](this) with Index[Set[String]]

  def fromRow(row: Row) : Movie = {
    Movie(
      id(row),
      title(row),
      year(row),
      genre(row)
    )
  }
}

object Movies extends Movies with CassandraConnector {
  def store(movie: Movie) : Future[ResultSet] = {
    insert
      .value(_.id, movie.movieId)
      .value(_.title, movie.title)
      .value(_.year, movie.year)
      .value(_.genre, movie.genre)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def getById(id: Int) : Future[Option[Movie]] = {
    select.where(_.id eqs id).one()
  }
}