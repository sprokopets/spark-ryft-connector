 // ============= Ryft-Customized BSD License ============
 // Copyright (c) 2015, Ryft Systems, Inc.
 // All rights reserved.
 // Redistribution and use in source and binary forms, with or without modification,
 // are permitted provided that the following conditions are met:
 //
 // 1. Redistributions of source code must retain the above copyright notice,
 //   this list of conditions and the following disclaimer.
 // 2. Redistributions in binary form must reproduce the above copyright notice,
 //   this list of conditions and the following disclaimer in the documentation and/or
 //   other materials provided with the distribution.
 // 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 //   This product includes software developed by Ryft Systems, Inc.
 // 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 //   to endorse or promote products derived from this software without specific prior written permission.
 //
 // THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 // EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 // WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 // DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 // DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 // (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 // LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 // ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 // (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 // SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 // ============

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
      "org.twitter4j"           %  "twitter4j-stream"             % twitter4jVersion,
      "io.spray"                %   "spray-json_2.10"             % sprayJson
    )}
)

lazy val examplesSettings = Seq(
  libraryDependencies ++= {
    val twitter4jVersion  = "3.0.3"
    Seq(
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
  settings(examplesSettings: _*).
  settings(
    name                := "examples"
  ).dependsOn(sparkRyftConnector)

assemblyJarName in assembly := "spark-ryft-connector.jar"