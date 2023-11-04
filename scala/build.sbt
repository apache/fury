name := "fury-scala"
scalaVersion := "2.13.12"
crossScalaVersions := Seq("2.12.18", "2.13.12", "3.3.1")
resolvers += Resolver.mavenLocal

val furyVersion = "0.4.0-SNAPSHOT"
libraryDependencies ++= Seq(
  "org.furyio" % "fury-core" % furyVersion,
  "org.scalatest" %% "scalatest" % "3.2.17",
)
