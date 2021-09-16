resolvers ++= Seq(
  "jgit-repo".at("https://download.eclipse.org/jgit/maven"),
  Resolver.url("bintray-sbt-plugin-releases", url("https://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
    Resolver.ivyStylePatterns
  )
)

addSbtPlugin("com.47deg" % "sbt-microsites" % "1.3.4")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.0.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.31")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")
