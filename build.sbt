lazy val root = (project in file(".")).
  settings(
    organization := "com.example",
    scalaVersion := "2.12.4",
    version      := "0.1.0-SNAPSHOT",
    name := "phenix-challenge",
    libraryDependencies ++= List(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"
    )
  )
