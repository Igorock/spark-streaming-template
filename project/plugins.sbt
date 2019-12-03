addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.aol.sbt" % "sbt-sonarrunner-plugin" % "1.0.4")
addSbtPlugin("com.github.sbtliquibase" % "sbt-liquibase" % "0.2.0")

resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/aol/scala"))(Resolver.ivyStylePatterns)