name := "scalastic"
 
version := "0.0.6-SNAPSHOT"

organization := "com.traackr"

crossScalaVersions := Seq("2.9.1", "2.9.2")
 
scalaVersion := "2.9.2" 


libraryDependencies += "org.elasticsearch" % "elasticsearch" % "0.19.8"
	
libraryDependencies += "org.scalaz" %% "scalaz-core" % "6.0.4"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.4"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.2"



libraryDependencies += "junit" % "junit" % "4.10" % "test"
	
libraryDependencies += "org.scalatest" %% "scalatest" % "1.6.1" % "test"



publishArtifact in Test := true

publishArtifact in Compile := true

parallelExecution in Test := false

resolvers += "sonatype releases" at "http://oss.sonatype.org/content/repositories/releases"
