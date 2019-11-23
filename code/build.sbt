name := "assn3-part2"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion