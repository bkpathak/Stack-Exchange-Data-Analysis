import sbt._
import Keys._

name := "Spark-Collections"

version := "1.0"

organization := "com.cloudwick"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  ("org.apache.hadoop" % "hadoop-client" % "2.2.0" % "provided").
    exclude("org.slf4j", "slf4j-log4j12").
    exclude("asm", "asm").
    exclude("org.jboss.netty", "netty").
    exclude("org.apache.hadoop", "hadoop-yarn-common"),
  ("org.apache.spark" %% "spark-core" % "1.2.0" % "provided").
    exclude("org.apache.hadoop", "hadoop-client").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("org.slf4j", "log4j12")
)