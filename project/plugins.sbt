resolvers ++= Seq(
  "jgit-repo" at "http://download.eclipse.org/jgit/maven",
  Resolver.url("bintray-sbt-plugin-releases",
    url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
 )

addSbtPlugin("com.47deg"          % "sbt-microsites"  % "0.7.0")
addSbtPlugin("com.eed3si9n"       % "sbt-unidoc"      % "0.4.1")
addSbtPlugin("com.github.gseitz"  % "sbt-release"     % "1.0.6")
addSbtPlugin("com.jsuereth"       % "sbt-pgp"         % "1.1.0")
addSbtPlugin("com.lucidchart"     % "sbt-scalafmt"    % "1.15")
addSbtPlugin("com.typesafe"       % "sbt-mima-plugin" % "0.1.18")
addSbtPlugin("com.typesafe.sbt"   % "sbt-ghpages"     % "0.6.2")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"   % "1.5.1")
addSbtPlugin("org.xerial.sbt"     % "sbt-sonatype"    % "2.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"         % "0.2.27")
