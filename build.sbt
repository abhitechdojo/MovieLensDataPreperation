name := "MovieLensDataPreperation"

version := "1.0"

scalaVersion := "2.11.7"

val PhantomVersion = "1.22.0"
val json4sVersion = "3.3.0"

libraryDependencies ++= Seq(
  "com.websudos" %% "phantom-dsl" % PhantomVersion,
  "joda-time" % "joda-time" % "2.9.1",
  "org.json4s" %% "json4s-native" % json4sVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion
)
//fork in run := true
//javaOptions in run += "-Xmx8G"