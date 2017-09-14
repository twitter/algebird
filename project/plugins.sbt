resolvers ++= Seq(
  "jgit-repo" at "http://download.eclipse.org/jgit/maven",
  Resolver.url("bintray-sbt-plugin-releases",
    url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
 )

addSbtPlugin("com.eed3si9n"       % "sbt-unidoc"      % "0.3.3")
addSbtPlugin("com.eed3si9n"       % "sbt-doge"        % "0.1.5")
addSbtPlugin("com.fortysevendeg"  % "sbt-microsites"  % "0.3.3")
addSbtPlugin("com.github.gseitz"  % "sbt-release"     % "1.0.6")
addSbtPlugin("com.jsuereth"       % "sbt-pgp"         % "1.1.0")
addSbtPlugin("com.typesafe"       % "sbt-mima-plugin" % "0.1.18")
addSbtPlugin("com.typesafe.sbt"   % "sbt-ghpages"     % "0.5.4")
addSbtPlugin("com.typesafe.sbt"   % "sbt-scalariform" % "1.3.0")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"   % "1.5.0")
addSbtPlugin("org.xerial.sbt"     % "sbt-sonatype"    % "1.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"         % "0.2.18")
