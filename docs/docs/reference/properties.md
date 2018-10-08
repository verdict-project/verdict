# VerdictDB JDBC Properties

VerdictDB supports a number of configurable properties, which can be written as `key=value` pairs inside JDBC connection string.
The currently supported properties are as follows:

* **verdictdbmetaschema**: sets the name of schema/database that VerdictDB will use to store metadata of scrambled tables.
* **verdictdbtempschema**: sets the name of schema/database that VerdictDB will use to create scrambled tables.
* **loglevel**: sets the minimum level of logs that VerdictDB will print out to the console. Possible values are {error, warn, info, debug, trace}.
* **file_loglevel**: sets the minimum level of logs that VerdictDB will print out to the log file . Possible values are {error, warn, info, debug, trace}.

For example, a JDBC connection string with VerdictDB-specific properties can be written as:

```java
String connectionString =
    String.format("jdbc:verdict:mysql://%s:%d/%s?verdictdbmetaschema=myverdictdbmeta&" +
        "verdictdbtempschema=myverdictdbtemp&loglevel=debug",
        MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE);
Connection vc = DriverManager.getConnection(connectionString, MYSQL_USER, MYSQL_PASSWORD);
```

With the above JDBC connection string,
VerdictDB will create metadata tables in the database `myverdictdbmeta` and scrambled tables in `myverdictdbtemp`, and 
every log with log level higher or equals to `debug` will be printed out to the console.