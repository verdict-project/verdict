# docker preparation commands
# docker run --rm -d --name verdictdb-mysql -p 127.0.0.1:3306:3306 -e MYSQL_DATABASE=test -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:5.5
# docker run --rm -d --name verdictdb-postgres -p 127.0.0.1:5432:5432 -e POSTGRES_DB=test -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="" postgres:10
# docker run --rm -d --name verdictdb-impala -p 127.0.0.1:21050:21050 codingtony/impala

FROM maven:3-jdk-8
WORKDIR /verdictdb
ADD . /verdictdb
CMD ["mvn", "test"]
