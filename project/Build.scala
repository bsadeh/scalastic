import sbt._
import sbt.Keys._
import scala._
import scala.Some

object Build extends Build {

  val basicSettings = Seq(
    organization := "org.scalastic",
    name := "scalastic",
    version := "0.90.10.1",
    description := "a scala driver for elasticsearch",
    homepage := Some(url("https://github.com/bsadeh/scalastic")),
    licenses := Seq("The Apache Software License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
  )

  val scalaSettings = Seq(
    scalaVersion := "2.10.3",
    scalacOptions ++= Seq("-unchecked", "-feature", "-deprecation")
  )

  val publishSettings = Seq(
    publishMavenStyle := true,
    publishArtifact in Compile := true,
    publishArtifact in Test := false,

    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },

    pomIncludeRepository := { _ => false },

    pomExtra :=
      <scm>
        <url>scm:git:git@github.com:bsadeh/scalastic.git</url>
        <connection>scm:git:git@github.com:bsadeh/scalastic.git</connection>
        <developerConnection>scm:git:git@github.com:bsadeh/scalastic.git</developerConnection>
      </scm>

      <developers>
        <developer>
          <id>bsadeh</id>
          <name>Benny Sadeh</name>
        </developer>
        <developer>
          <id>yatskevich</id>
          <name>Ivan Yatskevich</name>
        </developer>
      </developers>

      <issueManagement>
        <system>GitHub</system>
        <url>https://github.com/bsadeh/scalastic/issues/</url>
      </issueManagement>
  )

  lazy val Root = Project(id = "Root", base = file("."))
    .settings(basicSettings: _*)
    .settings(scalaSettings: _*)
    .settings(publishSettings: _*)
    .settings(
    resolvers += Resolver.sonatypeRepo("releases"),

    libraryDependencies ++= Seq(
      "org.elasticsearch" % "elasticsearch" % "1.1.0",
      "com.google.code.findbugs" % "jsr305" % "1.3.9",
      "org.clapper" %% "grizzled-slf4j" % "1.0.1",

      "junit" % "junit" % "4.10" % "test",
      "org.scalatest" %% "scalatest" % "2.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.0.2" % "test"
    ),

    parallelExecution in Test := false
  )

}
