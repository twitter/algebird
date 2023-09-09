resolvers ++= Seq(
  "jgit-repo".at("https://download.eclipse.org/jgit/maven"),
  Resolver.url("bintray-sbt-plugin-releases", url("https://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
    Resolver.ivyStylePatterns
  )
)

addSbtPlugin("com.47deg" % "sbt-microsites" % "1.4.3")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.8")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.5")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.0")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
