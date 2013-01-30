import sbt._
import Keys._

object StorehausBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ Seq(
    organization := "com.twitter",
    version := "0.1.8-SNAPSHOT",
    scalaVersion := "2.9.2",

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
        <url>git@github.com:twitter/scalding.git</url>
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
    base = file(".")
    ).settings(
    test := { }
  ).aggregate(algebirdTesting,
              algebirdCore)

  lazy val algebirdTesting = Project(
    id = "algebird-testing",
    base = file("algebird-testing"),
    settings = sharedSettings
  ).settings(
    name := "algebird-testing",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0",
      "org.scala-tools.testing" % "specs_2.9.1" % "1.6.9"
    )
  ).dependsOn(algebirdCore % "compile->compile")

  lazy val algebirdCore = Project(
    id = "algebird-core",
    base = file("algebird-core"),
    settings = sharedSettings
  ).settings(
    name := "algebird-core",
    libraryDependencies += "com.googlecode.javaewah" % "JavaEWAH" % "0.6.6"
  ).dependsOn(algebirdTesting % "test->test")
}
