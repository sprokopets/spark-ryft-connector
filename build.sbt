lazy val commonSettings = Seq(
  organization        := "com.ryft",
  version             := "1.0",
  scalaVersion        := "2.11.7",
  libraryDependencies ++= {
    val akkaVersion       = "2.4-M2"
    val sparkVersion      = "1.4.1"
    val scalajVersion     = "1.1.5"
    val logbackVersion    = "1.0.12"
    val json4sVersion     = "3.3.0.RC3"
    val twitter4jVersion  = "3.0.3"
    Seq(
    "com.typesafe.akka"       %  "akka-actor_2.11"              % akkaVersion,
    "com.typesafe.akka"       %  "akka-slf4j_2.11"              % akkaVersion,
    "org.apache.spark"        %  "spark-core_2.11"              % sparkVersion,
    "org.apache.spark"        %  "spark-streaming_2.11"         % sparkVersion,
    "org.apache.spark"        %  "spark-streaming-twitter_2.11" % sparkVersion,
    "org.scalaj"              %  "scalaj-http_2.11"             % scalajVersion,
    "ch.qos.logback"          %  "logback-classic"              % logbackVersion,
    "org.json4s"              %  "json4s-native_2.11"           % json4sVersion,
    "org.twitter4j"           %  "twitter4j-stream"             % twitter4jVersion
  )}
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector-root"
  )

lazy val sparkRyftConnector = (project in file("spark-ryft-connector")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector"
  )

lazy val sparkRyftConnectorJava = (project in file("spark-ryft-connector-java")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector-java"
  ).
  dependsOn(root,sparkRyftConnector)

lazy val examples = (project in file("examples")).
  settings(commonSettings: _*).
  settings(
    name                := "examples"
  ).
  dependsOn(root,sparkRyftConnector,sparkRyftConnectorJava)