#!/bin/bash
VERDICTDB_JDBC_DRIVER_NAME=org.verdictdb.jdbc41.Driver

# Read version
# used prefix and suffix removing operators in bash
# https://stackoverflow.com/questions/16623835/remove-a-fixed-prefix-suffix-from-a-string-in-bash
property_version_line=$(head pom.xml | grep version | tr -d '[:space:]')
verdictdb_version=${property_version_line#$"<version>"}
verdictdb_version=${verdictdb_version%$"</version>"}
#echo "${verdictdb_version}"

# Package
mvn -DskipTests -Dverdictdb-packaging=true package

# Unzip the packaged file
JAR_FILE_NAME=target/verdictdb-core-${verdictdb_version}-jar-with-dependencies.jar
DRIVER_FILE_PATH=META-INF/services/java.sql.Driver
unzip -p ${JAR_FILE_NAME} ${DRIVER_FILE_PATH} > ./java.sql.Driver

# Read service file
SERVICE_FILE_NAME=./java.sql.Driver
driver_name=$(cat ${SERVICE_FILE_NAME})

if [ ${driver_name} == ${VERDICTDB_JDBC_DRIVER_NAME} ]; then
    echo "VerdictDB JDBC service file is CORRECT."
    exit 0
else
    echo "VerdictDB JDBC service file is INCORRECT!!"
    exit 1
fi
