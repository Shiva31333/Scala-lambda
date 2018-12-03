import sbt.Keys._
import sbt._
import sbtrelease.Version

name := "Alert_Summary_Report"

resolvers += Resolver.sonatypeRepo("public")
scalaVersion := "2.12.6"
releaseNextVersion := { ver => Version(ver).map(_.bumpMinor.string).getOrElse("Error") }
assemblyJarName in assembly := "Alert_Summary_Report.jar"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.2.0",
  "commons-io" % "commons-io" % "2.3",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "mysql" % "mysql-connector-java" % "8.0.13",
  "com.amazonaws" % "aws-lambda-java-events" % "2.2.1",
  "com.amazonaws" % "aws-lambda-java-core" % "1.2.0",
  "com.google.code.gson" % "gson" % "2.2.4",
  "com.typesafe.play" %% "play-json" % "2.6.10",
  "io.spray" %% "spray-json" % "1.3.5"
)

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Xfatal-warnings")
