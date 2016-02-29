package com.abhi.model

import com.abhi.connector.CassandraConnector
import com.abhi.entities.User
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._

import scala.concurrent.Future

class Users extends CassandraTable[Users, User] {
  object id extends IntColumn(this) with PartitionKey[Int]
  object age extends IntColumn(this) with Index[Int]
  object gender extends StringColumn(this) with Index[String]
  object occupation extends StringColumn(this) with Index[String]
  object zipCode extends StringColumn(this) with Index[String]

  def fromRow(row: Row) : User = {
    User(
      id(row),
      gender(row),
      age(row),
      occupation(row),
      zipCode(row)
    )
  }
}

object Users extends Users with CassandraConnector {
  def store(user: User) : Future[ResultSet] = {
    insert
      .value(_.id, user.id)
      .value(_.gender, user.gender)
      .value(_.age, user.age)
      .value(_.occupation, user.occupation)
      .value(_.zipCode, user.zipCode)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def getById(id: Int) : Future[Option[User]] = {
    select.where(_.id eqs id).one()
  }
}