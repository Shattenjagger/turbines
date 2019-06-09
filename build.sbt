name := "backend-test-task"

version := "1.0"

scalaVersion := "2.12.8"

lazy val akkaVersion = "2.5.15"

libraryDependencies ++= Seq(
  "com.beachape" %% "enumeratum" % "1.5.13",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

libraryDependencies += "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.5.15.1"
javaOptions in Test += s"-Dconfig.file=${sourceDirectory.value}/test/resources/application.test.conf"
fork in Test := true

resolvers += "dnvriend" at "http://dl.bintray.com/dnvriend/maven"
