# Setup MySQL / MariaDB

If you already have your own MySQL database set up, you can skip this step and proceed with the next step to load TPC-H data.

In this tutorial, we will set up the MySQL database using Docker.


## Docker Installation

In this tutorial, we will launch MariaDB (i.e., open-source fork of MySQL), which is essentially equivalent to MySQL (at least for our tutorial), using Docker.

Docker is an open-source project for developers to create, deploy and run any application using containers.

You can download Docker [here](https://www.docker.com/community-edition#/download), and simply follow their instructions to install it.


## Start Your Database

After you install Docker, you can launch a MySQL/MariaDB instance in a Docker container by running the following command:

```bash
docker run --rm -d --name verdictdb-mysql -p 127.0.0.1:3306:3306 \
-e MYSQL_DATABASE=test -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mariadb:10
```

This command launches a MySQL/MariaDB instance locally using the default port of 3306.
You can access this database as a user 'root' without passwords.


!!! warning "Port is already allocated?"
    You may receive the error like blow when starting the docker container.

    > userland proxy: Bind for 0.0.0.0:3306 failed: port is already allocated

    This error simply means that there is already a process listening on the port 3306, e.g., already existing MySQL instance on your machine. You can either stop/terminate the MySQL instance and run the above docker command again. Or, you can simply use another port, e.g., 3305, to avoid the port conflict as follows:
    ```bash
    docker run --rm -d --name verdictdb-mysql -p 127.0.0.1:3305:3306 \
    -e MYSQL_DATABASE=test -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mariadb:10
    ```
