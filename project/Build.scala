package algebird

import sbt._
import Keys._

import com.typesafe.sbt.SbtSite.{ site, SiteKeys }
import com.typesafe.sbt.SbtGhPages.ghpages
import com.typesafe.sbt.SbtGit.GitKeys.gitRemoteRepo

object AlgebirdBuild extends Build {
  lazy val unidocSettings: Seq[sbt.Setting[_]] =
    site.includeScaladoc("target/site") ++ Seq(
      scalacOptions in doc <++= (version, baseDirectory in LocalProject("algebird")).map { (v, rootBase) =>
        val tagOrBranch = if (v.endsWith("-SNAPSHOT")) "develop" else v
        val docSourceUrl = "https://github.com/twitter/algebird/tree/" + tagOrBranch + "€{FILE_PATH}.scala"
        Seq("-sourcepath", rootBase.getAbsolutePath, "-doc-source-url", docSourceUrl)
      },
      Unidoc.unidocDirectory <<= target/ "site",
      gitRemoteRepo := "git@github.com:twitter/algebird.git"
    )

  lazy val publishSettings = site.settings ++ Unidoc.settings ++ ghpages.settings ++ unidocSettings

  val sharedSettings = Project.defaultSettings ++ Seq(
    organization := "com.twitter",
    version := "0.1.9-SNAPSHOT",
    scalaVersion := "2.9.2",
    crossScalaVersions := Seq("2.9.2", "2.10.0"),

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases"
    ),

    parallelExecution in Test := true,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

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
  )

  lazy val algebird = Project(
    id = "algebird",
    base = file("."),
    settings = sharedSettings ++ publishSettings
    ).settings(
    test := { }
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
    libraryDependencies += "com.googlecode.javaewah" % "JavaEWAH" % "0.6.6"
  )

  lazy val algebirdTest = Project(
    id = "algebird-test",
    base = file("algebird-test"),
    settings = sharedSettings
  ).settings(
    name := "algebird-test",
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
    resolvers += "Twitter Maven" at "http://maven.twttr.com",
    libraryDependencies += "com.twitter" % "util-core" % "5.3.15"
  ).dependsOn(algebirdCore, algebirdTest % "compile->test")
}
