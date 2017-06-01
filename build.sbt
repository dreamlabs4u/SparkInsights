val projectName = "SparkInsights"

val dependencies = Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
// test
  "org.scalatest" %% "scalatest" % "3.0.1" % "provided"
)

parallelExecution in Test := false
   
lazy val main = Project(projectName, base = file("."))
  .settings(libraryDependencies ++= dependencies)
  .settings(scalaVersion := "2.11.8")


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
