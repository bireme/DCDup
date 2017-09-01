lazy val commonSettings = Seq(
  organization := "br.bireme",
  version := "0.1.0",
  scalaVersion := "2.12.3" //"2.12.1"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "DCDup"
  )

val luceneVersion = "6.6.0"
val jacksonVersion = "2.9.0"//"2.8.9"
val httpClientVersion = "4.5.3"
val mySQLVersion = "5.1.42"
//val mySQLVersion = "6.0.6"
val logBackVersion = "1.2.3"
val circeParserVersion = "0.8.0"
val scalaTestVersion = "3.0.4" //"3.0.3"
val hairyfotrVersion = "0.1.17"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-suggest" % luceneVersion,
  "org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "org.apache.httpcomponents" % "httpclient" % httpClientVersion,
  "mysql" % "mysql-connector-java" % mySQLVersion,
  "ch.qos.logback" % "logback-classic" % logBackVersion,
  "io.circe" % "circe-parser_2.12" % circeParserVersion,
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

logBuffered in Test := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")
addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % hairyfotrVersion)
