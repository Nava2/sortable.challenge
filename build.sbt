

val jacksonVer = "2.6.7"

lazy val root = (project in file(".")).
  settings(
    name := "brightwell.sortable.challenge",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.0.1",
      "com.jsuereth" %% "scala-arm" % "1.4",
      "com.typesafe.play" %% "play-json" % "2.5.9",
      "com.github.nscala-money" %% "nscala-money" % "0.11.0",
      "com.github.nscala-money" %% "nscala-money-play-json" % "0.11.0",
      "com.github.scopt" %% "scopt" % "3.5.0"
    ),
    dependencyOverrides ++= Set(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVer,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVer,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVer
//      "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.6"
    )
  )