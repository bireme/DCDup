lazy val commonSettings = Seq(
  organization := "br.bireme",
  version := "0.1.0",
  scalaVersion := "2.12.6" //"2.12.4"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "DCDup"
  )

val commonsLangVersion = "3.7"
val commonsTextVersion = "1.4" //"1.3"
val luceneVersion = "7.4.0" //"7.3.1"
val jacksonVersion = "2.9.6" //"2.9.5"
val httpClientVersion = "4.5.5" //"4.5.3"
val mySQLVersion = "8.0.11" // "8.0.8-dmr"
val logBackVersion = "1.2.3"
val circeParserVersion = "0.9.3" //"0.8.0"
val scalaTestVersion = "3.0.5"
val hairyfotrVersion = "0.1.17"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-lang3" % commonsLangVersion,
  "org.apache.commons" % "commons-text" % commonsTextVersion,
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
trapExit := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")
addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % hairyfotrVersion)
