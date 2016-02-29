package com.abhi.connector

import com.datastax.driver.core.Cluster
import com.websudos.phantom.connectors.{KeySpace, SessionProvider}
import com.websudos.phantom.dsl._

trait CassandraConnector extends SessionProvider {
  implicit val space: KeySpace = Connector.keyspace
  val cluster = Connector.cluster
  override implicit lazy val session: Session = Connector.session
}

object Connector {
  val keyspace: KeySpace = new KeySpace("MovieLens")

  val cluster =
    Cluster.builder()
      .addContactPoints("172.17.0.9")
      //.withCredentials(config.getString("cassandra.username"), config.getString("cassandra.password"))
      .build()

  val session: Session = cluster.connect(keyspace.name)
}
