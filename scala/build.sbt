name := "fury-scala"
scalaVersion := "2.13.12"
crossScalaVersions := Seq("2.12.18", "2.13.12", "3.3.1")
resolvers += Resolver.mavenLocal

val furyVersion = "0.3.0-SNAPSHOT"
libraryDependencies ++= Seq(
  "org.furyio" % "fury-core" % furyVersion,
  "org.scalatest" %% "scalatest" % "3.2.17",
)

Test / unmanagedSourceDirectories ++= {
  if (scalaBinaryVersion.value.startsWith("2.12")) {
    Seq(
      (LocalRootProject / baseDirectory).value / "src" / "test" / "scala-2.12"
    )
  } else {
    Seq(
      (LocalRootProject / baseDirectory).value / "src" / "test" / "scala-2.13+"
    )
  }
}