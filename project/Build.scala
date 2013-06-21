package algebird

import sbt._
import Keys._
import sbtgitflow.ReleasePlugin._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object AlgebirdBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ releaseSettings ++ Seq(
    organization := "com.twitter",
    scalaVersion := "2.9.2",
    crossScalaVersions := Seq("2.9.2", "2.10.0"),

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases"
    ),

    parallelExecution in Test := true,

    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),

    // Publishing options:
    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("sonatype-releases"  at nexus + "service/local/staging/deploy/maven2")
    },

    pomExtra := (
      <url>https://github.com/twitter/algebird</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:twitter/algebird.git</url>
        <connection>scm:git:git@github.com:twitter/algebird.git</connection>
      </scm>
      <developers>
        <developer>
          <id>oscar</id>
          <name>Oscar Boykin</name>
          <url>http://twitter.com/posco</url>
        </developer>
        <developer>
          <id>sritchie</id>
          <name>Sam Ritchie</name>
          <url>http://twitter.com/sritchie</url>
        </developer>
      </developers>)
  ) ++ mimaDefaultSettings

  // This returns the youngest jar we released that is compatible with the current
  def youngestForwardCompatible(subProj: String) =
    Some("com.twitter" % ("algebird-" + subProj + "_2.9.2") % "0.1.12")

  lazy val algebird = Project(
    id = "algebird",
    base = file("."),
    settings = sharedSettings ++ DocGen.publishSettings
    ).settings(
    test := { },
    publish := { }, // skip publishing for this root project.
    publishLocal := { }
  ).aggregate(algebirdTest,
              algebirdCore,
              algebirdUtil)

  lazy val algebirdCore = Project(
    id = "algebird-core",
    base = file("algebird-core"),
    settings = sharedSettings
  ).settings(
    test := { }, // All tests reside in algebirdTest
    name := "algebird-core",
    previousArtifact := youngestForwardCompatible("core"),
    libraryDependencies ++= Seq(
      "com.googlecode.javaewah" % "JavaEWAH" % "0.6.6",
      "com.googlecode.efficient-java-matrix-library" % "ejml" % "0.22"
    )
  )

  lazy val algebirdTest = Project(
    id = "algebird-test",
    base = file("algebird-test"),
    settings = sharedSettings
  ).settings(
    name := "algebird-test",
    previousArtifact := youngestForwardCompatible("test"),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0",
      "org.scala-tools.testing" %% "specs" % "1.6.9"
    )
  ).dependsOn(algebirdCore)

  lazy val algebirdUtil = Project(
    id = "algebird-util",
    base = file("algebird-util"),
    settings = sharedSettings
  ).settings(
    name := "algebird-util",
    previousArtifact := youngestForwardCompatible("util"),
    libraryDependencies += "com.twitter" %% "util-core" % "6.2.0"
  ).dependsOn(algebirdCore, algebirdTest % "compile->test")
}
