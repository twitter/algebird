import ReleaseTransformations._
import algebird._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import pl.project13.scala.sbt.JmhPlugin
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

val algebraVersion = "0.7.0"
val bijectionVersion = "0.9.4"
val javaEwahVersion = "1.1.4"
val paradiseVersion = "2.1.0"
val quasiquotesVersion = "2.1.0"
val scalaTestVersion = "3.0.1"
val scalacheckVersion = "1.13.4"
val utilVersion = "6.20.0"
val utilVersion212 = "6.39.0"

def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
  case version if version startsWith "2.10" => "2.10"
  case version if version startsWith "2.11" => "2.11"
  case version if version startsWith "2.12" => "2.12"
  case _ => sys.error("unknown error")
}

def isScala210x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.10"
def isScala212x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.12"

/**
  * Remove 2.10 projects from doc generation, as the macros used in the projects
  * cause problems generating the documentation on scala 2.10. As the APIs for 2.10
  * and 2.11 are the same this has no effect on the resultant documentation, though
  * it does mean that the scaladocs cannot be generated when the build is in 2.10 mode.
  *
  * TODO: enable algebirdSpark when we turn on the algebirdSpark 2.12 build.
  */
def docsSourcesAndProjects(sv: String): (Boolean, Seq[ProjectReference]) =
  CrossVersion.partialVersion(sv) match {
    case Some((2, 10)) => (false, Nil)
    case _ => (true, Seq(
      algebirdTest,
      algebirdCore,
      algebirdUtil,
      algebirdBijection
      // algebirdSpark
    ))
  }

// TODO: figure out the correct value for the autoformat
val sharedSettings = scalariformSettings(autoformat = true) ++  Seq(
  organization := "com.twitter",
  scalaVersion := "2.11.11",
  crossScalaVersions := Seq("2.10.6", "2.11.11", "2.12.3"),
  ScalariformKeys.preferences := formattingPreferences,

  resolvers ++= Seq(
    Opts.resolver.sonatypeSnapshots,
    Opts.resolver.sonatypeReleases
  ),

  parallelExecution in Test := true,

  javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),

  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials"),

  scalacOptions ++= {
    if (scalaVersion.value startsWith "2.10")
      Seq("-Xdivergence211")
    else
      Seq()
  },

  scalacOptions ++= {
    if (scalaVersion.value startsWith "2.12")
      Seq("-opt:l:inline", "-opt-inline-from:com.twitter.algebird.**")
    else
      Seq("-optimize")
  },

  javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),

  libraryDependencies += "junit" % "junit" % "4.11" % "test",

  // Publishing options:
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseVersionBump := sbtrelease.Version.Bump.Minor, // need to tweak based on mima results
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { x => false },

  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    releaseStepCommandAndRemaining("+test"), // formerly runTest, here to deal with algebird-spark
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishSigned"), // formerly publishArtifacts, here to deal with algebird-spark
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = releaseStepCommand("sonatypeReleaseAll")),
    pushChanges),


  publishTo := Some(
      if (version.value.trim.endsWith("SNAPSHOT"))
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
    ),

  scmInfo := Some(
    ScmInfo(
      url("https://github.com/twitter/algebird"),
      "scm:git@github.com:twitter/algebird.git"
    )
  ),

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

lazy val noPublishSettings = Seq(
    publish := {},
    publishLocal := {},
    test := {},
    publishArtifact := false
  )

/**
  * This returns the youngest jar we released that is compatible with
  * the current.
  */
val noBinaryCompatCheck = Set[String]("benchmark", "caliper", "generic")

def youngestForwardCompatible(subProj: String) =
  Some(subProj)
    .filterNot(noBinaryCompatCheck.contains(_))
    .map { s => "com.twitter" %% ("algebird-" + s) % "0.13.0" }

lazy val algebird = Project(
  id = "algebird",
  base = file("."))
  .settings(sharedSettings)
  .settings(noPublishSettings)
  .settings(coverageExcludedPackages := "<empty>;.*\\.benchmark\\..*")
  .aggregate(
  algebirdTest,
  algebirdCore,
  algebirdUtil,
  algebirdBijection,
  algebirdBenchmark,
  algebirdGeneric
  //algebirdSpark
)

def module(name: String) = {
  val id = "algebird-%s".format(name)
  Project(id = id, base = file(id)).settings(sharedSettings ++ Seq(
    Keys.name := id,
    mimaPreviousArtifacts := youngestForwardCompatible(name).toSet)
  )
}

lazy val algebirdCore = module("core").settings(
  initialCommands := """
                     import com.twitter.algebird._
                     """.stripMargin('|'),
  libraryDependencies ++=
    Seq("com.googlecode.javaewah" % "JavaEWAH" % javaEwahVersion,
        "org.typelevel" %% "algebra" % algebraVersion,
        "org.scala-lang" % "scala-reflect" % scalaVersion.value,
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test") ++ {
      if (isScala210x(scalaVersion.value))
        Seq("org.scalamacros" %% "quasiquotes" % quasiquotesVersion)
      else
        Seq()
    },
  sourceGenerators in Compile += Def.task {
      GenTupleAggregators.gen((sourceManaged in Compile).value)
    }.taskValue,
  addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full),

  // Scala 2.12's doc task was failing.
  sources in (Compile, doc) ~= (_ filterNot (_.absolutePath.contains("javaapi")))
)

lazy val algebirdTest = module("test").settings(
  testOptions in Test ++= Seq(Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "4")),
  libraryDependencies ++=
    Seq("org.scalacheck" %% "scalacheck" % scalacheckVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion) ++ {
      if (isScala210x(scalaVersion.value))
        Seq("org.scalamacros" %% "quasiquotes" % quasiquotesVersion)
      else
        Seq()
    }, addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
).dependsOn(algebirdCore)

lazy val algebirdBenchmark = module("benchmark")
  .settings(JmhPlugin.projectSettings:_*)
  .settings(noPublishSettings)
  .settings(
   coverageExcludedPackages := "com\\.twitter\\.algebird\\.benchmark.*",
   libraryDependencies ++= Seq("com.twitter" %% "bijection-core" % bijectionVersion)
).dependsOn(algebirdCore, algebirdUtil, algebirdTest % "test->compile").enablePlugins(JmhPlugin)

lazy val algebirdUtil = module("util").settings(
    libraryDependencies ++= {
      val utilV =
        if (isScala212x(scalaVersion.value)) utilVersion212 else utilVersion
      Seq("com.twitter" %% "util-core" % utilV)
    }
).dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val algebirdBijection = module("bijection").settings(
  libraryDependencies += "com.twitter" %% "bijection-core" % bijectionVersion
).dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val algebirdSpark = module("spark").settings(
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
    crossScalaVersions := crossScalaVersions.value.filterNot(_.startsWith("2.12"))
  ).dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val algebirdGeneric = module("generic").settings(
    addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % "2.3.3",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.6")
  ).dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val docsMappingsAPIDir = settingKey[String]("Name of subdirectory in site target directory for api docs")

lazy val docSettings = Seq(
  micrositeName := "Algebird",
  micrositeDescription := "Abstract Algebra for Scala.",
  micrositeAuthor := "Algebird's contributors",
  micrositeHighlightTheme := "atom-one-light",
  micrositeHomepage := "http://twitter.github.io/algebird",
  micrositeBaseUrl := "algebird",
  micrositeDocumentationUrl := "api",
  micrositeGithubOwner := "twitter",
  micrositeExtraMdFiles := Map(file("CONTRIBUTING.md") -> microsites.ExtraMdFileConfig("contributing.md", "contributing")),
  micrositeGithubRepo := "algebird",
  micrositePalette := Map(
    "brand-primary" -> "#5B5988",
    "brand-secondary" -> "#292E53",
    "brand-tertiary" -> "#222749",
    "gray-dark" -> "#49494B",
    "gray" -> "#7B7B7E",
    "gray-light" -> "#E5E5E6",
    "gray-lighter" -> "#F4F3F4",
    "white-color" -> "#FFFFFF"),
  autoAPIMappings := true,
  unidocProjectFilter in (ScalaUnidoc, unidoc) :=
    inProjects(docsSourcesAndProjects(scalaVersion.value)._2:_*),
  docsMappingsAPIDir := "api",
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), docsMappingsAPIDir),
  ghpagesNoJekyll := false,
  fork in tut := true,
  fork in (ScalaUnidoc, unidoc) := true,
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-doc-source-url", "https://github.com/twitter/algebird/tree/develop€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-diagrams"
  ),
  git.remoteRepo := "git@github.com:twitter/algebird.git",
  includeFilter in makeSite := "*.html" | "*.css" | "*.png" | "*.jpg" | "*.gif" | "*.js" | "*.swf" | "*.yml" | "*.md"
)

// Documentation is generated for projects defined in
// `docsSourcesAndProjects`.
lazy val docs = project
  .enablePlugins(MicrositesPlugin, TutPlugin, ScalaUnidocPlugin, GhpagesPlugin)
  .settings(moduleName := "algebird-docs")
  .settings(sharedSettings)
  .settings(noPublishSettings)
  .settings(docSettings)
  .settings((scalacOptions in Tut) ~= (_.filterNot(Set("-Ywarn-unused-import", "-Ywarn-dead-code"))))
  .dependsOn(algebirdCore)
