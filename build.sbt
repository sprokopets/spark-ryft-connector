import sbt.Keys._

lazy val commonSettings = Seq(
  organization        := "com.ryft",
  version             := "1.0",
  scalaVersion        := "2.10.5",
  libraryDependencies ++= {
    val akkaVersion       = "2.3.12"
    val sparkVersion      = "1.4.1"
    val scalajVersion     = "1.1.5"
    val logbackVersion    = "1.0.12"
    val json4sVersion     = "3.3.0.RC3"
    val twitter4jVersion  = "3.0.3"
    val sprayJson         = "1.3.2"
    Seq(
    "com.typesafe.akka"       %  "akka-actor_2.10"              % akkaVersion   % "provided",
    "com.typesafe.akka"       %  "akka-slf4j_2.10"              % akkaVersion   % "provided",
    "org.apache.spark"        %  "spark-core_2.10"              % sparkVersion  % "provided",
    "org.apache.spark"        %  "spark-streaming_2.10"         % sparkVersion  % "provided",
    "org.apache.spark"        %  "spark-streaming-twitter_2.10" % sparkVersion,
    "org.scalaj"              %  "scalaj-http_2.10"             % scalajVersion,
    "ch.qos.logback"          %  "logback-classic"              % logbackVersion,
    "org.json4s"              %  "json4s-native_2.10"           % json4sVersion,
    "org.json4s"              %  "json4s-core_2.10"             % json4sVersion,
    "org.twitter4j"           %  "twitter4j-stream"             % twitter4jVersion
    )}
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class")
  => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector-root"
  ).
  dependsOn(sparkRyftConnector,examples)

lazy val sparkRyftConnector = (project in file("spark-ryft-connector")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector"
  )

lazy val sparkRyftConnectorJava = (project in file("spark-ryft-connector-java")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector-java"
  )

lazy val examples = (project in file("examples")).
  settings(commonSettings: _*).
  settings(
    name                := "examples"
  ).dependsOn(sparkRyftConnector)

assemblyJarName in assembly := "spark-ryft-connector.jar"