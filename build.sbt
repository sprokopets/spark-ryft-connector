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

version := "0.1.0"

lazy val commonSettings = Seq(
  organization        := "com.ryft",
  scalaVersion        := "2.10.5",
  libraryDependencies ++= {
    val akkaVersion       = "2.3.12"
    val sparkVersion      = "1.4.1"
    val scalajVersion     = "1.1.5"
    val logbackVersion    = "1.0.12"
    val json4sVersion     = "3.3.0.RC3"
    val twitter4jVersion  = "3.0.3"
    val sprayJsonVersion  = "1.3.2"
    val msgpackVersion    = "0.7.0-p9"
    Seq(
      "com.typesafe.akka"       %  "akka-actor_2.10"              % akkaVersion,
      "org.apache.spark"        %  "spark-core_2.10"              % sparkVersion % "provided",
      "org.apache.spark"        %  "spark-streaming_2.10"         % sparkVersion % "provided",
      "org.apache.spark"        %  "spark-sql_2.10"               % sparkVersion % "provided",
      "org.apache.spark"        %  "spark-streaming-twitter_2.10" % sparkVersion % "provided",
      "org.scalaj"              %  "scalaj-http_2.10"             % scalajVersion,
      "ch.qos.logback"          %  "logback-classic"              % logbackVersion,
      "org.json4s"              %  "json4s-native_2.10"           % json4sVersion,
      "org.json4s"              %  "json4s-core_2.10"             % json4sVersion,
      "org.twitter4j"           %  "twitter4j-stream"             % twitter4jVersion,
      "io.spray"                %  "spray-json_2.10"              % sprayJsonVersion,
      "org.msgpack"             %  "jackson-dataformat-msgpack"   % msgpackVersion
    )}
)

lazy val examplesSettings = Seq(
  libraryDependencies ++= {
    val twitter4jVersion  = "3.0.3"
    Seq(
      "org.twitter4j"           %  "twitter4j-stream"             % twitter4jVersion
    )}
)

lazy val sparkRyftConnectorSettings = Seq(
  libraryDependencies ++= {
    val jacksonVersion    = "2.4.4"
    val scalaTestVersion  = "3.0.0-M7"
    val sprayVersion      = "1.3.3"
    val specs2Version     = "2.4.15"
    val junitVersion      = "4.12"
    Seq(
      "com.fasterxml.jackson.module"  % "jackson-module-scala_2.10" % jacksonVersion,
      "org.scalatest"                 % "scalatest_2.10"            % scalaTestVersion % "test",
      "io.spray"                      % "spray-can_2.10"            % sprayVersion     % "test",
      "io.spray"                      % "spray-routing_2.10"        % sprayVersion     % "test",
      "io.spray"                      % "spray-testkit_2.10"        % sprayVersion     % "test",
      "org.specs2"                    % "specs2-core_2.10"          % specs2Version    % "test",
      "junit"                         % "junit"                     % junitVersion     % "test"
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
  dependsOn(sparkRyftConnector,sparkRyftConnectorJava,examples)

lazy val sparkRyftConnector = (project in file("spark-ryft-connector")).
  settings(commonSettings: _*).
  settings(sparkRyftConnectorSettings: _*).
  settings(
    name                := "spark-ryft-connector"
  )

lazy val sparkRyftConnectorJava = (project in file("spark-ryft-connector-java")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector-java"
  ).dependsOn(sparkRyftConnector)

lazy val examples = (project in file("examples")).
  settings(commonSettings: _*).
  settings(examplesSettings: _*).
  settings(
    name                := "examples"
  ).dependsOn(sparkRyftConnector,sparkRyftConnectorJava)

assemblyJarName in assembly := s"spark-ryft-connector_2.10-${version.value}.jar"