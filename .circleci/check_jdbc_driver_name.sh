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
TEMP_DIR=target/jdbc-service-file-check
rm -rf ${TEMP_DIR}
mkdir ${TEMP_DIR}
unzip -q target/verdictdb-core-${verdictdb_version}-jar-with-dependencies.jar -d ${TEMP_DIR}

# Read service file
SERVICE_FILE_NAME=${TEMP_DIR}/META-INF/services/java.sql.Driver
driver_name=$(cat ${SERVICE_FILE_NAME})

if [ ${driver_name} == ${VERDICTDB_JDBC_DRIVER_NAME} ]; then
    echo "VerdictDB JDBC service file is CORRECT."
    exit 0
else
    echo "VerdictDB JDBC service file is INCORRECT!!"
    exit 1
fi
