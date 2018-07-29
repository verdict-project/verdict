# Installation

## Maven

Add following lines to your maven dependency list to use VerdictDB.

```pom
<!-- https://mvnrepository.com/artifact/org.verdictdb/verdictdb-core -->
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>0.5.0-alpha</version>
</dependency>

```

## pip

This is in preparation.


## Download a Compiled Jar

You only need a single Jar file.

Download: [verdictdb-core-0.5.0-alpha-jar-with-dependencies.jar](https://github.com/mozafari/verdictdb/releases/download/v0.5.0-alpha/verdictdb-core-0.5.0-alpha-jar-with-dependencies.jar)


## Build Yourself

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
    mvn package -Dmaven.test.skip=true
    ```
    It will download the dependencies for VerdictDB and skip the tests. It will generate the jar file
    `verdictdb-core-0.5.0-alpha.jar` under `target` directory.


