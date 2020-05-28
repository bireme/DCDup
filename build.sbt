lazy val commonSettings = Seq(
  organization := "br.bireme",
  version := "0.1.0",
  scalaVersion := /*"2.12.9"*/ "2.13.2" // binarios nao funcionam quando da execucao
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "DCDup"
  )

val commonsLangVersion = "3.10" //"3.9"
val commonsTextVersion = "1.8" //"1.7"
val luceneVersion = "8.5.1" //"8.4.1"
//val jacksonVersion = /*"2.9.9.3"*/ "2.9.9"
val httpClientVersion = "4.5.12" //"4.5.11"
val scalajHttpVersion = "2.4.2" //"2.4.1"
val mySQLVersion = "8.0.20" //"8.0.19"
val logBackVersion = "1.2.3"
val circeVersion = "0.13.0" //"0.12.3"
val scalaTestVersion = "3.1.2" //"3.1.0"
val akkaVersion =  "2.6.5" //"2.6.3"
val xerces2Version = "2.12.0"
//val hairyfotrVersion = "0.1.17"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-lang3" % commonsLangVersion,
  "org.apache.commons" % "commons-text" % commonsTextVersion,
  "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-suggest" % luceneVersion,
  "org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
  //"com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  //"com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  //"com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "org.apache.httpcomponents" % "httpclient" % httpClientVersion,
  "org.scalaj" %% "scalaj-http" % scalajHttpVersion,
  "mysql" % "mysql-connector-java" % mySQLVersion,
  "ch.qos.logback" % "logback-classic" % logBackVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "xerces" % "xercesImpl" % xerces2Version
)

//test in assembly := {}

logBuffered in Test := false
trapExit := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")
//addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % hairyfotrVersion)
