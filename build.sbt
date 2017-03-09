lazy val commonSettings = Seq(
  organization := "br.bireme",
  version := "0.1.0",
  scalaVersion := "2.12.1"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "DCDup"
  )

val luceneVersion = "6.4.2"
//val luceneVersion = "6.3.0"
//val luceneVersion = "6.2.1"
//val luceneVersion = "6.1.0"
val jacksonVersion = "2.7.8"
//val json4sVersion = "3.5.0"
val httpClientVersion = "4.5.2"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-suggest" % luceneVersion,
  "org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "org.apache.httpcomponents" % "httpclient" % httpClientVersion
  //"org.json4s" %% "json4s-native" % json4sVersion
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
