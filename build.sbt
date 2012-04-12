name := "scalastic"
 
version := "0.0.1"
 
scalaVersion := "2.9.1" 


libraryDependencies += "org.elasticsearch" % "elasticsearch" % "0.19.1"
	
libraryDependencies += "net.liftweb" %% "lift-json" % "2.4"
	
libraryDependencies += "net.liftweb" %% "lift-json-ext" % "2.4"
	
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.6.2"

libraryDependencies += "junit" % "junit" % "4.10" % "test"
	
libraryDependencies += "org.scalatest" %% "scalatest" % "1.6.1" % "test"
