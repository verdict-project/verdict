# VerdictDB on Apache Spark


We will build a Scala application. Note that Scala is the language Spark is written in. To build our example application, the following tools must have been installed.

1. sbt: [sbt installation guide](https://www.scala-sbt.org/1.0/docs/Setup.html)


## Create an empty project

The following command creates a project that prints out "hello".

```bash
$ sbt new sbt/scala-seed.g8

A minimal Scala project.

name [Scala Seed Project]: hello-verdict

Template applied in ./hello-verdict
```

Move into the project directory: `cd hello-verdict`.

Remove the `src/test` directory, which we do not need: `rm -rf src/test`.



## Configure build setting

Add the following line in `build.sbt`, under the existing `import Dependencies._` line. As of the time of writing, the latest version of Apache Spark only supports Scala 2.11.

```scala
scalaVersion := "2.11.1"
```

Also, replace the existing dependency list with

```scala
libraryDependencies ++= Seq(
  scalaTest % Test,
  "org.verdictdb" % "verdictdb-core" % "0.5.4",
  "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
)
```

## Package and Submit

```bash
$ sbt package
$ spark-submit target/scala-2.11/Hello-assembly-0.1.0-SNAPSHOT.jar
```
