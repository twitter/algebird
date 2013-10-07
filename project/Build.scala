package algebird

import sbt._
import Keys._
import sbtgitflow.ReleasePlugin._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object AlgebirdBuild extends Build {
  def withCross(dep: ModuleID) =
    dep cross CrossVersion.binaryMapped {
      case "2.9.3" => "2.9.2" // TODO: hack because twitter hasn't built things against 2.9.3
      case version if version startsWith "2.10" => "2.10" // TODO: hack because sbt is broken
      case x => x
    }

  val sharedSettings = Project.defaultSettings ++ releaseSettings ++ Seq(
    organization := "com.twitter",
    scalaVersion := "2.9.3",
    crossScalaVersions := Seq("2.9.3", "2.10.0"),

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

  /**
    * This returns the youngest jar we released that is compatible with
    * the current.
    */
  val unreleasedModules = Set[String]()

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map { s => "com.twitter" % ("algebird-" + s + "_2.9.3") % "0.2.0" }

  lazy val algebird = Project(
    id = "algebird",
    base = file("."),
    settings = sharedSettings ++ DocGen.publishSettings
    ).settings(
    test := { },
    publish := { }, // skip publishing for this root project.
    publishLocal := { }
  ).aggregate(
    algebirdTest,
    algebirdCore,
    algebirdUtil,
    algebirdBijection
  )

  def module(name: String) = {
    val id = "algebird-%s".format(name)
    Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
      Keys.name := id,
      previousArtifact := youngestForwardCompatible(name))
    )
  }

  lazy val algebirdCore = module("core").settings(
    test := { }, // All tests reside in algebirdTest
    initialCommands := """
                       import com.twitter.algebird._
                       """.stripMargin('|'),
    libraryDependencies += "com.googlecode.javaewah" % "JavaEWAH" % "0.6.6",
    sourceGenerators in Compile <+= sourceManaged in Compile map { outDir: File =>
      GenTupleAggregators.gen(outDir)
    }
  )

  lazy val algebirdTest = module("test").settings(
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0",
      "org.scala-tools.testing" %% "specs" % "1.6.9"
    )
  ).dependsOn(algebirdCore)

  lazy val algebirdUtil = module("util").settings(
    libraryDependencies += withCross("com.twitter" %% "util-core" % "6.3.0")
  ).dependsOn(algebirdCore, algebirdTest % "test->compile")

  lazy val algebirdBijection = module("bijection").settings(
    libraryDependencies += "com.twitter" %% "bijection-core" % "0.5.2"
  ).dependsOn(algebirdCore, algebirdTest % "test->compile")
}
