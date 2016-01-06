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
  version := "0.4.0",
  scalaVersion        := "2.10.5",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),
  libraryDependencies ++= {
    val akkaVersion       = "2.3.12"
    val sparkVersion      = "1.4.1"
    val scalajVersion     = "1.1.5"
    val logbackVersion    = "1.0.12"
    val json4sVersion     = "3.3.0.RC3"
    val twitter4jVersion  = "3.0.3"
    val sprayJsonVersion  = "1.3.2"
    val msgpackVersion    = "0.7.0-p9"
    val configVersion     = "1.3.0"
    val jodaTimeVersion   = "2.9"
    val commonsIOVersion  = "1.3.2"
    Seq(
      "com.typesafe.akka"       %%  "akka-actor"              % akkaVersion,
      "org.apache.spark"        %%  "spark-core"              % sparkVersion % "provided",
      "org.apache.spark"        %%  "spark-streaming"         % sparkVersion % "provided",
      "org.apache.spark"        %%  "spark-sql"               % sparkVersion % "provided",
      "org.apache.spark"        %%  "spark-streaming-twitter" % sparkVersion % "provided",
      "org.scalaj"              %%  "scalaj-http"             % scalajVersion,
      "ch.qos.logback"          %  "logback-classic"              % logbackVersion,
      "org.json4s"              %%  "json4s-native"           % json4sVersion,
      "org.json4s"              %%  "json4s-core"             % json4sVersion,
      "org.twitter4j"           %  "twitter4j-stream"             % twitter4jVersion,
      "io.spray"                %%  "spray-json"              % sprayJsonVersion,
      "org.msgpack"             %  "jackson-dataformat-msgpack"   % msgpackVersion,
      "com.typesafe"            %  "config"                       % configVersion,
      "joda-time"               %  "joda-time"                    % jodaTimeVersion,
      "org.apache.commons"      %  "commons-io"                   % commonsIOVersion
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
      "com.fasterxml.jackson.module"  %% "jackson-module-scala" % jacksonVersion,
      "org.scalatest"                 %% "scalatest"            % scalaTestVersion % "test",
      "io.spray"                      %% "spray-can"            % sprayVersion     % "test",
      "io.spray"                      %% "spray-routing"        % sprayVersion     % "test",
      "io.spray"                      %% "spray-testkit"        % sprayVersion     % "test",
      "org.specs2"                    %% "specs2-core"          % specs2Version    % "test",
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
    name                := "spark-ryft-connector"
  ).
  dependsOn(sparkRyftConnectorCore,sparkRyftConnectorJava,examples).
  aggregate(sparkRyftConnectorCore,sparkRyftConnectorJava,examples).
  settings(
    aggregate in assembly := false
  )

lazy val sparkRyftConnectorCore = (project in file("spark-ryft-connector")).
  settings(commonSettings: _*).
  settings(sparkRyftConnectorSettings: _*).
  settings(
    name                := "spark-ryft-connector-core"
  )

lazy val sparkRyftConnectorJava = (project in file("spark-ryft-connector-java")).
  settings(commonSettings: _*).
  settings(
    name                := "spark-ryft-connector-java"
  ).dependsOn(sparkRyftConnectorCore)

lazy val examples = (project in file("examples")).
  settings(commonSettings: _*).
  settings(examplesSettings: _*).
  settings(
    name                := "spark-ryft-connector-examples"
  ).dependsOn(sparkRyftConnectorCore,sparkRyftConnectorJava)



assemblyJarName in assembly := s"spark-ryft-connector-${scalaVersion.value}-${version.value}.jar"