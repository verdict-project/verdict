# Download / Install

Depending on your language preference (Java or Python), you can use different approaches.

*Note: Python interface is currently in preparation.*


## Java

### Maven

If you use [Apache Maven](https://maven.apache.org/) for your project's dependency management, including the following dependency item is all you need to do to use VerdictDB.

```pom
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>0.5.0-alpha</version>
</dependency>

```

### Download a Pre-compiled Jar

You only need a single jar file. This jar file is compiled with JDK8.

**Download**: [verdictdb-core-0.5.0-alpha-jar-with-dependencies.jar](https://github.com/mozafari/verdictdb/releases/download/v0.5.0-alpha/verdictdb-core-0.5.0-alpha-jar-with-dependencies.jar)


### Build Yourself

1. **Clone** from our [Github public repository](https://github.com/mozafari/verdictdb). Use command
    ```
    git clone https://github.com/mozafari/verdictdb.git
    ```
2. **Change** directory to the directory to the repository you have cloned. Use command,
    ```
    cd verdictdb
    ```

3. **Build** the jar file by maven. Use command
    ```
    mvn -DskipTests -DtestPhase=false -DpackagePhase=true clean package
    ```
    Check the `target` directory for the created jar files.


## Python

This is in preparation.
