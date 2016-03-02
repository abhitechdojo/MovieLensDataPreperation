-- this cql script is intended to be run against cassandra instances.

DROP KEYSPACE if exists MovieLens;

CREATE KEYSPACE MovieLens WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

use MovieLens;

drop table if exists Movies;
drop table if exists Users;
drop table if exists MovieRatings;

create table Movies (
  id int primary key,
  title text,
  year int,
  genre set<text>
);

create index on Movies(year);
create index on Movies(genre);

create table Users (
  id int primary key,
  gender text,
  age int,
  occupation text,
  zipCode text
);

create index on Users(gender);
create index on Users(age);
create index on Users(occupation);
create index on Users(zipCode);

create table Ratings (
  userId int,
  movieId int,
  rating int,
  ratingTimestamp timestamp,
  primary key ((userId, movieId), ratingTimestamp)
);

create index on Ratings(rating);
create index on Ratings(ratingTimestamp);