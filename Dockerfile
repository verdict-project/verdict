FROM maven:3-jdk-8

CMD ["mvn", "test"]

# docker run --rm -p 127.0.0.1:3306:3306 --name verdictdb-mysql -e MYSQL_DATABASE=test -e MYSQL_ALLOW_EMPTY_PASSWORD="yes" mysql:5.5
# docker run --rm -p 127.0.0.1:5432:5432 --name verdictdb-postgres -e POSTGRES_DB=test -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD="" postgres:10
# mysql -uroot
