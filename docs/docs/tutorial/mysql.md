# Setup MySQL/MariaDB for VerdictDB Tutorial (Optional)

If you already have your own MySQL database set up, you can skip this step and proceed with the next step to load TPC-H data.

This tutorial allows you to set up your own MySQL database using Docker.

## Docker Installation

In this tutorial, we will launch MariaDB (i.e., open-source fork of MySQL), which is essentially equivalent to MySQL (at least for our tutorial), using Docker.

Docker is an open-source project for developers to create, deploy and run any application using containers.

You can download Docker [here](https://www.docker.com/community-edition#/download), and simply follow their instructions to install it.

## Start Your Database

After you install Docker, you can launch a MySQL/MariaDB instance in a Docker container by running the following command:

```bash
$ docker run --rm -d --name verdictdb-mysql -p 127.0.0.1:3306:3306 -e MYSQL_DATABASE=test -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mariadb:10
```

This command launches a MySQL/MariaDB instance locally using the default port of 3306.
You can access this database as a user 'root' without passwords.

if you receive error like
```
userland proxy: Bind for 0.0.0.0:3306 failed: port is already allocated
```
that means the port 3306 has been occupied. One possible reason is MySQL has already taken this port. You can resolve the error by turning off your host machine MySQL Damon.



Wasn't that easy? Now, you are ready to proceed to the next stage of this tutorial!