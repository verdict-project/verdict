#!/bin/bash
VERDICTDB_JDBC_DRIVER_NAME=org.verdictdb.jdbc41.Driver

mvn -DskipTests -Dverdictdb-packaging=true package
TEMP_DIR=target/jdbc-service-file-check
SERVICE_FILE_NAME=${TEMP_DIR}/META-INF/services/java.sql.Driver
PROPERTY_FILE=${TEMP_DIR}/META-INF/maven/org.verdictdb/verdictdb-core/pom.properties

# read version
# used prefix removing operator in bash
# https://stackoverflow.com/questions/16623835/remove-a-fixed-prefix-suffix-from-a-string-in-bash
property_version_line=$(cat ${PROPERTY_FILE} | grep version)
verdictdb_version=${property_version_line#$"version="}
#echo "${verdictdb_version}"

# read service file
rm -rf ${TEMP_DIR}
mkdir ${TEMP_DIR}
unzip -q target/verdictdb-core-${verdictdb_version}-jar-with-dependencies.jar -d ${TEMP_DIR}
driver_name=$(cat ${SERVICE_FILE_NAME})

if [ ${driver_name} == ${VERDICTDB_JDBC_DRIVER_NAME} ]; then
    exit 0
else
    exit 1
fi
