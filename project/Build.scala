package algebird

import sbt._
import Keys._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact
import cappi.Plugin._
import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform._

object AlgebirdBuild extends Build {

  val paradiseVersion = "2.0.1"
  val quasiquotesVersion = "2.0.1"

  def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
    case version if version startsWith "2.10" => "2.10"
    case version if version startsWith "2.11" => "2.11"
    case _ => sys.error("unknown error")
  }
  def isScala210x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.10"

  val sharedSettings = Project.defaultSettings ++ scalariformSettings ++  Seq(
    organization := "com.twitter",
    scalaVersion := "2.10.5",
    crossScalaVersions := Seq("2.10.5", "2.11.7"),
    ScalariformKeys.preferences := formattingPreferences,

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases"
    ),

    parallelExecution in Test := true,

    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),

    scalacOptions ++= Seq("-unchecked", "-deprecation", "-language:implicitConversions", "-language:higherKinds", "-language:existentials"),

    scalacOptions <++= (scalaVersion) map { sv =>
        if (sv startsWith "2.10")
          Seq("-Xdivergence211")
        else
          Seq()
    },

    javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),

    libraryDependencies += "junit" % "junit" % "4.11" % "test",

    // Publishing options:
    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version { v =>
      Some(
        if (v.trim.endsWith("SNAPSHOT"))
          Opts.resolver.sonatypeSnapshots
        else
          Opts.resolver.sonatypeStaging
      )
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


  lazy val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().
      setPreference(AlignParameters, false).
      setPreference(PreserveSpaceBeforeArguments, true)
  }

  /**
    * This returns the youngest jar we released that is compatible with
    * the current.
    */
  val unreleasedModules = Set[String]()

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map { s => "com.twitter" % ("algebird-" + s + "_2.10") % "0.11.0" }

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
    algebirdBijection,
    algebirdSpark
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
    libraryDependencies <++= (scalaVersion) { scalaVersion =>
      Seq("com.googlecode.javaewah" % "JavaEWAH" % "0.6.6",
          "org.scala-lang" % "scala-reflect" % scalaVersion) ++ {
        if (isScala210x(scalaVersion))
          Seq("org.scalamacros" %% "quasiquotes" % quasiquotesVersion)
        else
          Seq()
      }
    },
    sourceGenerators in Compile <+= sourceManaged in Compile map { outDir: File =>
      GenTupleAggregators.gen(outDir)
    }, addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
  )

  lazy val algebirdTest = module("test").settings(
    libraryDependencies <++= (scalaVersion) { scalaVersion =>
      Seq("org.scalacheck" %% "scalacheck" % "1.11.5",
          "org.scalatest" %% "scalatest" % "2.2.2") ++ {
        if (isScala210x(scalaVersion))
          Seq("org.scalamacros" %% "quasiquotes" % quasiquotesVersion)
        else
          Seq()
      }
    }, addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
  ).dependsOn(algebirdCore)

  /** Uses https://github.com/softprops/cappi#readme
   * Note, a bug in cappi mis-reports the benchmark
   * names.
   *
   * use cappi::benchmarkOnly com.twitter.algebird.caliper.HllBenchmark
   */
  lazy val algebirdCaliper = module("caliper").settings(
     libraryDependencies ++= Seq("com.twitter" %% "bijection-core" % "0.8.0"),
      javaOptions in run <++= (fullClasspath in Runtime) map { cp => Seq("-cp", sbt.Build.data(cp).mkString(":")) },
      fork in run := true
    ).settings(cappiSettings : _*).dependsOn(algebirdCore, algebirdUtil, algebirdTest % "test->compile")

  lazy val algebirdUtil = module("util").settings(
    libraryDependencies += "com.twitter" %% "util-core" % "6.20.0"
  ).dependsOn(algebirdCore, algebirdTest % "test->test")

  lazy val algebirdBijection = module("bijection").settings(
    libraryDependencies += "com.twitter" %% "bijection-core" % "0.8.0"
  ).dependsOn(algebirdCore, algebirdTest % "test->test")

  lazy val algebirdSpark = module("spark").settings(
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
  ).dependsOn(algebirdCore, algebirdTest % "test->test")
}


