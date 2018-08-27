# Download/Install

Use a different interface depending on your language preference (Java or Python).

*Note: Python interface is currently in preparation.*


## Java

One of the following three methods can be used:

1. Maven
1. Using a pre-compiled jar
1. Build yourself

### Maven

If you use [Apache Maven](https://maven.apache.org/) for your project's dependency management, adding the following dependency entry to your `pom.xml` is all you need to do to use VerdictDB.

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

    Since Maven requires JDK. Please make sure you have installed JDK. You can use command
    ```
    javac -version
    ```
    to check your java compiler version. The output should look like this
    ```
    $ java -version
    openjdk version "1.8.0_171-1-ojdkbuild"
    OpenJDK Runtime Environment (build 1.8.0_171-1-ojdkbuild-b10)
    OpenJDK 64-Bit Server VM (build 25.171-b10, mixed mode)
    ```
    If not, please install JDK first and then use maven to build the file.


## Python

This is in preparation.
