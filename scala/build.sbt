name := "fury-scala"
scalaVersion := "2.13.13"
crossScalaVersions := Seq("2.13.12", "3.3.3")
resolvers += Resolver.mavenLocal

val furyVersion = "0.5.0-rc1"
libraryDependencies ++= Seq(
  "org.apache.fury" % "fury-core" % furyVersion,
  "org.scalatest" %% "scalatest" % "3.2.17",
)
