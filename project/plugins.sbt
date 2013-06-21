resolvers ++= Seq(
  "jgit-repo" at "http://download.eclipse.org/jgit/maven",
  "sonatype-releases"  at "http://oss.sonatype.org/content/repositories/releases"
)

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.0")

addSbtPlugin("com.twitter" % "sbt-gitflow" % "0.1.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.5")
