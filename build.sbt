name := "spaif"

version := "0.1"

scalaVersion := "2.11.12"

javaOptions += "-Xmx4G"

// http://www.scalatest.org/
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"


val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6"
)

// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-xml
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.0"


// https://mvnrepository.com/artifact/org.json4s/json4s-native
libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

// https://mvnrepository.com/artifact/org.jsoup/jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"

// https://mvnrepository.com/artifact/org.scalaj/scalaj-http
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.1"

// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.12"
