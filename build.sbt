name := "DeltaLake"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "2.4.3" % Test
libraryDependencies += "io.delta" %% "delta-core" % "0.3.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.15"
libraryDependencies += "com.pivotal" % "greenplum-jdbc" % "5.1.4"
