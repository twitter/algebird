import algebird._
import com.typesafe.tools.mima.core._
import pl.project13.scala.sbt.JmhPlugin

val algebraVersion = "2.0.0"
val bijectionVersion = "0.9.7"
val javaEwahVersion = "1.1.7"
val kindProjectorVersion = "0.11.3"
val paradiseVersion = "2.1.1"
val quasiquotesVersion = "2.1.0"
val scalaTestVersion = "3.2.6"
val scalaTestPlusVersion = "3.1.0.0-RC2"
val scalacheckVersion = "1.15.2"
val scalaCollectionCompat = "2.4.3"
val utilVersion = "21.2.0"
val sparkVersion = "2.4.7"

def scalaVersionSpecificFolders(srcBaseDir: java.io.File, scalaVersion: String) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, y)) if y <= 12 =>
      new java.io.File(s"${srcBaseDir.getPath}-2.12-") :: Nil
    case Some((2, y)) if y >= 13 =>
      new java.io.File(s"${srcBaseDir.getPath}-2.13+") :: Nil
    case _ => Nil
  }

def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
  case version if version.startsWith("2.11") => "2.11"
  case version if version.startsWith("2.12") => "2.12"
  case version if version.startsWith("2.13") => "2.13"
  case version                               => sys.error(s"unsupported scala version $version")
}

def isScala212x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.12"
def isScala213x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.13"

noPublishSettings
crossScalaVersions := Nil

val sharedSettings = Seq(
  organization := "com.twitter",
  scalaVersion := "2.12.13",
  crossScalaVersions := Seq("2.11.12", scalaVersion.value),
  resolvers ++= Seq(
    Opts.resolver.sonatypeSnapshots,
    Opts.resolver.sonatypeReleases
  ),
  parallelExecution in Test := true,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials"
  ),
  scalacOptions ++= {
    VersionNumber(scalaVersion.value) match {
      case v if v.matchesSemVer(SemanticSelector("<2.12")) =>
        Seq("-Ywarn-unused", "-Ywarn-unused-import", "-optimize")
      case v if v.matchesSemVer(SemanticSelector(">=2.12")) =>
        val ops = Seq("-Ywarn-unused", "-opt:l:inline", "-opt-inline-from:com.twitter.algebird.**")
        if (v.matchesSemVer(SemanticSelector("2.13.x"))) {
          "-Ymacro-annotations" +: ops
        } else {
          ops
        }
      case _ =>
        Nil
    }
  },
  javacOptions ++= Seq("-target", "1.6", "-source", "1.6"),
  libraryDependencies ++= Seq(
    "junit" % "junit" % "4.13.2" % Test,
    "com.novocode" % "junit-interface" % "0.11" % Test
  ),
  // Publishing options:
  pomIncludeRepository := { x => false },
  pomExtra := (<url>https://github.com/twitter/algebird</url>
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
    </developers>),
  Compile / unmanagedSourceDirectories ++= scalaVersionSpecificFolders(
    (Compile / scalaSource).value,
    scalaVersion.value
  ),
  Test / unmanagedSourceDirectories ++= scalaVersionSpecificFolders(
    (Test / scalaSource).value,
    scalaVersion.value
  ),
  Compile / unmanagedSourceDirectories ++= scalaVersionSpecificFolders(
    (Compile / javaSource).value,
    scalaVersion.value
  )
) ++ mimaSettings

lazy val noPublishSettings = Seq(
  publish / skip := true,
  publish := {},
  publishLocal := {},
  test := {},
  publishArtifact := false
)

// https://github.com/lightbend/mima/issues/388
lazy val mimaSettings = Def.settings(
  mimaBinaryIssueFilters ++= Seq(
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.IdentityMonad.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.Averager.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.AveragedGroup.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.MomentsGroup.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.MomentsAggregator.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.NullGroup.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.BigIntRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.BitSetLite.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.BooleanRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.JBoolRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.DoubleRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.JDoubleRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.JShortRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.ShortRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.IntRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.JIntRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.FloatRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.JFloatRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.LongRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.JLongRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.BigDecimalRing.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.UnitGroup.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.HLLSeries.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.SparseHLL.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.DenseHLL.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.StringMonoid.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.OrValMonoid.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.AndValMonoid.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.DecayedValueMonoid.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.MinHashSignature.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.statistics.PlainCounter.*"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.statistics.AtomicCounter.*"),
    ProblemFilters
      .exclude[DirectMissingMethodProblem]("com.twitter.algebird.statistics.GaussianDistributionMonoid.*"),
    ProblemFilters
      .exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.CompactionSize.apply"),
    ProblemFilters
      .exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.FlushFrequency.apply"),
    ProblemFilters
      .exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.MemoryFlushPercent.apply"),
    ProblemFilters
      .exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.UpdateFrequency.apply"),
    ProblemFilters
      .exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.RollOverFrequency.apply"),
    ProblemFilters
      .exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.HeavyHittersPercent.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.BufferSize.apply"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.twitter.algebird.util.summer.Compact.apply"),
    ProblemFilters.exclude[IncompatibleSignatureProblem]("com.twitter.algebird.MinHashSignature.*"),
    ProblemFilters.exclude[IncompatibleSignatureProblem]("com.twitter.algebird.MinHasher.*")
  )
)

/**
 * This returns the previous jar we released that is compatible with
 * the current.
 */
val noBinaryCompatCheck = Set[String]("benchmark", "caliper", "spark")

def previousVersion(subProj: String) =
  Some(subProj)
    .filterNot(noBinaryCompatCheck.contains)
    .map(s => "com.twitter" %% ("algebird-" + s) % "0.13.7")

lazy val algebird = Project(id = "algebird", base = file("."))
  .settings(sharedSettings)
  .settings(noPublishSettings)
  .settings(
    coverageExcludedPackages := "<empty>;.*\\.benchmark\\..*",
    mimaFailOnNoPrevious := false
  )
  .aggregate(
    algebirdCore,
    algebirdTest,
    algebirdUtil,
    algebirdBijection,
    algebirdBenchmark,
    algebirdGeneric,
    algebirdSpark
  )

def module(name: String) = {
  val id = "algebird-%s".format(name)
  Project(id = id, base = file(id))
    .settings(sharedSettings ++ Seq(Keys.name := id, mimaPreviousArtifacts := previousVersion(name).toSet))
}

lazy val algebirdCore = module("core").settings(
  crossScalaVersions += "2.13.4",
  initialCommands := """
                     import com.twitter.algebird._
                     """.stripMargin('|'),
  libraryDependencies ++=
    Seq(
      "com.googlecode.javaewah" % "JavaEWAH" % javaEwahVersion,
      "org.typelevel" %% "algebra" % algebraVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompat
    ) ++ {
      if (isScala213x(scalaVersion.value)) {
        Seq()
      } else {
        Seq(compilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full)))
      }
    },
  addCompilerPlugin(("org.typelevel" % "kind-projector" % kindProjectorVersion).cross(CrossVersion.full)),
  sourceGenerators in Compile += Def.task {
    GenTupleAggregators.gen((sourceManaged in Compile).value)
  }.taskValue,
  // Scala 2.12's doc task was failing.
  sources in (Compile, doc) ~= (_.filterNot(_.absolutePath.contains("javaapi"))),
  testOptions in Test := Seq(Tests.Argument(TestFrameworks.JUnit, "-a"))
)

lazy val algebirdTest = module("test")
  .settings(
    testOptions in Test ++= Seq(Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "4")),
    crossScalaVersions += "2.13.4",
    libraryDependencies ++=
      Seq(
        "org.scalacheck" %% "scalacheck" % scalacheckVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersion,
        "org.scalatestplus" %% "scalatestplus-scalacheck" % scalaTestPlusVersion % "test"
      ) ++ {
        if (isScala213x(scalaVersion.value)) {
          Seq()
        } else {
          Seq(compilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full)))
        }
      },
    addCompilerPlugin(
      ("org.typelevel" % "kind-projector" % kindProjectorVersion).cross(CrossVersion.full)
    )
  )
  .dependsOn(algebirdCore)

lazy val algebirdBenchmark = module("benchmark")
  .settings(JmhPlugin.projectSettings: _*)
  .settings(noPublishSettings)
  .settings(
    coverageExcludedPackages := "com\\.twitter\\.algebird\\.benchmark.*",
    libraryDependencies ++= Seq("com.twitter" %% "bijection-core" % bijectionVersion)
  )
  .dependsOn(algebirdCore, algebirdUtil, algebirdTest % "test->compile")
  .enablePlugins(JmhPlugin)

lazy val algebirdUtil = module("util")
  .settings(
    crossScalaVersions += "2.13.4",
    libraryDependencies ++= Seq("com.twitter" %% "util-core" % utilVersion)
  )
  .dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val algebirdBijection = module("bijection")
  .settings(
    crossScalaVersions += "2.13.4",
    libraryDependencies += "com.twitter" %% "bijection-core" % bijectionVersion
  )
  .dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val algebirdSpark = module("spark")
  .settings(
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    scalacOptions := scalacOptions.value
      .filterNot(
        _.contains("inline")
      ) // Disable optimizations for now: https://github.com/scala/bug/issues/11247
  )
  .dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val algebirdGeneric = module("generic")
  .settings(
    crossScalaVersions += "2.13.4",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % "2.3.3",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5"
    ) ++ {
      if (isScala213x(scalaVersion.value)) {
        Seq()
      } else {
        Seq(compilerPlugin(("org.scalamacros" % "paradise" % paradiseVersion).cross(CrossVersion.full)))
      }
    }
  )
  .dependsOn(algebirdCore, algebirdTest % "test->test")

lazy val docsMappingsAPIDir = settingKey[String]("Name of subdirectory in site target directory for api docs")

lazy val docSettings = Seq(
  micrositeName := "Algebird",
  micrositeDescription := "Abstract Algebra for Scala.",
  micrositeAuthor := "Algebird's contributors",
  micrositeHighlightTheme := "atom-one-light",
  micrositeHomepage := "https://twitter.github.io/algebird",
  micrositeBaseUrl := "algebird",
  micrositeDocumentationUrl := "api",
  micrositeGithubOwner := "twitter",
  micrositeExtraMdFiles := Map(
    file("CONTRIBUTING.md") -> microsites.ExtraMdFileConfig("contributing.md", "contributing")
  ),
  micrositeGithubRepo := "algebird",
  micrositePalette := Map(
    "brand-primary" -> "#5B5988",
    "brand-secondary" -> "#292E53",
    "brand-tertiary" -> "#222749",
    "gray-dark" -> "#49494B",
    "gray" -> "#7B7B7E",
    "gray-light" -> "#E5E5E6",
    "gray-lighter" -> "#F4F3F4",
    "white-color" -> "#FFFFFF"
  ),
  autoAPIMappings := true,
  docsMappingsAPIDir := "api",
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), docsMappingsAPIDir),
  ghpagesNoJekyll := false,
  fork in (ScalaUnidoc, unidoc) := true,
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-doc-source-url",
    "https://github.com/twitter/algebird/tree/develop€{FILE_PATH}.scala",
    "-sourcepath",
    baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-diagrams"
  ),
  git.remoteRepo := "git@github.com:twitter/algebird.git",
  includeFilter in makeSite := "*.html" | "*.css" | "*.png" | "*.jpg" | "*.gif" | "*.js" | "*.swf" | "*.yml" | "*.md"
)

// Documentation is generated for projects defined in
// `docsSourcesAndProjects`.
lazy val docs = project
  .enablePlugins(MicrositesPlugin, MdocPlugin, ScalaUnidocPlugin, GhpagesPlugin)
  .settings(moduleName := "algebird-docs")
  .settings(sharedSettings)
  .settings(noPublishSettings)
  .settings(docSettings)
  .settings(
    addCompilerPlugin(("org.typelevel" % "kind-projector" % kindProjectorVersion).cross(CrossVersion.full)),
    mdocIn := sourceDirectory.value / "main" / "mdoc",
    sources in (ScalaUnidoc, unidoc) ~= (_.filterNot(_.absolutePath.contains("javaapi")))
  )
  .dependsOn(algebirdCore)
