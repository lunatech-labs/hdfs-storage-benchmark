// ------------------------------
// APPLICATION DESCRIPTION

name := "hdfs-storage-benchmark"

version := "0.1.0-SNAPSHOT"

isSnapshot := true

description := "A simple benchmarking tool for various serializers available in Spark, e.g. Avro vs Parquet vs...."

// ------------------------------
// VERSIONS CONFIGURATION

scalaVersion := "2.10.4"

val sparkVersion = "1.5.2"

// ------------------------------
// DEPENDENCIES AND RESOLVERS

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion force(),
  "org.apache.spark" %% "spark-sql" % sparkVersion force(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion force()
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

libraryDependencies ++= Seq(
  "spark.jobserver" %% "job-server-api" % "0.6.1",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "com.databricks" %% "spark-avro" % "2.0.1"
//  ,"org.apache.hive" % "spark-client" % "2.0.0"
)

// ------------------------------
// RUNNING

// Make sure that provided dependencies are added to classpath when running in sbt
run in Compile <<= Defaults.runTask(fullClasspath in Compile,
  mainClass in(Compile, run),
  runner in(Compile, run))

fork in run := true

// ------------------------------
// ASSEMBLY
assemblyJarName in assembly := s"${name.value}-fat.jar"

// Exclude thee unmanaged library jars from local-lib
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.absolutePath.startsWith(unmanagedBase.value.getAbsolutePath)}
}

// Add exclusions, provided...
assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)


// ------------------------------
