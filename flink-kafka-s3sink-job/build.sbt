ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

val circeVersion = "0.14.2"
val flinkVersion = "1.20.0"

lazy val root = (project in file("."))
  .settings(
    name := "flink-kafka-s3sink-job",
    version := "0.1",
    scalaVersion := "2.13.14",
    resolvers += Resolver.mavenLocal,
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
      "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
      "org.apache.flink" % "flink-walkthrough-common" % flinkVersion,
      "org.apache.flink" % "flink-table-planner-loader" % flinkVersion,
      "org.apache.flink" % "flink-table-common" % flinkVersion,
      "org.apache.flink" % "flink-table-api-java" % flinkVersion,
      "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion,
      "org.apache.flink" % "flink-table-runtime" % flinkVersion,
      "org.apache.flink" % "flink-json" % flinkVersion,
      "org.apache.flink" % "flink-connector-base" % flinkVersion,
      "org.apache.flink" % "flink-statebackend-rocksdb" % flinkVersion,
      "org.apache.flink" % "flink-statebackend-changelog" % flinkVersion,
      "org.apache.flink" % "flink-statebackend-heap-spillable" % flinkVersion,
      "org.apache.flink" % "flink-s3-fs-hadoop" % flinkVersion,
      "org.apache.flink" % "flink-azure-fs-hadoop" % flinkVersion
    )
      ++ Seq(
        "org.apache.logging.log4j" % "log4j-api" % "2.24.0",
        "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.26.1",
        "org.apache.logging.log4j" % "log4j-core" % "2.24.0"
      ).map(_ % "provided")
      ++ Seq(
        "org.apache.flink" % "flink-connector-kafka" % "3.2.0-1.19",
        "org.apache.kafka" % "kafka-clients" % "3.8.0"
      )
      ++ Seq(
        "io.circe" %% "circe-core",
        "io.circe" %% "circe-generic",
        "io.circe" %% "circe-parser",
        "io.circe" %% "circe-generic-extras"
      ).map(_ % circeVersion),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.last
    },
    assembly / mainClass := Some("demo.flink.Main"),
    // make run command include the provided dependencies
    Compile / run := Defaults
      .runTask(
        Compile / fullClasspath,
        Compile / run / mainClass,
        Compile / run / runner
      )
      .evaluated,
    // stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
    Compile / run / fork := true,
    Global / cancelable := true,
    // exclude Scala library from assembly
    assemblyPackageScala / assembleArtifact := false,
    // or as follows
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    }
  )
