name := "topnanalysis"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
 
)


//test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

//resolvers += Resolver.sonatypeRepo("public")
resolvers += Resolver.mavenLocal

parallelExecution in Test := false

//test in assembly := {}
