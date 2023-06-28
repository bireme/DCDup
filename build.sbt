lazy val commonSettings = Seq(
  organization := "br.bireme",
  version := "0.1.0",
  scalaVersion := /*"2.12.9"*/ "2.13.10" // binarios nao funcionam quando da execucao
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "DCDup"
  )

val commonsLangVersion = "3.12.0" //"3.11"
val commonsTextVersion = "1.10.0" //"1.8"
val luceneVersion = "9.7.0" /*"9.5.0" "6.0.0" "8.6.0" */ /*"8.5.1"*/
//val jacksonVersion = /*"2.9.9.3"*/ "2.9.9"
val httpClientVersion = "4.5.14" //"4.5.12"
val scalajHttpVersion = "2.4.2" //"2.4.1"
val mySQLVersion = "8.0.32" //"8.0.21"
val logBackVersion = "1.4.5" //"1.2.3"
val circeVersion = "0.14.4" //"0.13.0"
val scalaTestVersion = "3.2.15" //"3.2.0"
val akkaVersion = "2.7.0" //"2.6.8"
val xerces2Version = "2.12.2" //"2.12.0"
val stringDistanceVersion = "1.2.7" //"1.2.3"
val googlePatchVersion = "20121119" //"895a9512bb"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-lang3" % commonsLangVersion,
  "org.apache.commons" % "commons-text" % commonsTextVersion,
  //"org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.apache.lucene" % "lucene-analysis-common" % luceneVersion,
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-suggest" % luceneVersion,
  "org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
  "org.apache.lucene" % "lucene-codecs" % luceneVersion % Test,
  "org.apache.httpcomponents" % "httpclient" % httpClientVersion,
  "org.scalaj" %% "scalaj-http" % scalajHttpVersion,
  "mysql" % "mysql-connector-java" % mySQLVersion,
  "ch.qos.logback" % "logback-classic" % logBackVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "xerces" % "xercesImpl" % xerces2Version,
  "com.github.vickumar1981" %% "stringdistance" % stringDistanceVersion,
  "org.webjars" % "google-diff-match-patch" % googlePatchVersion
)

//test in assembly := {}

Test / logBuffered := false
trapExit := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-unused")
//addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % hairyfotrVersion)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
