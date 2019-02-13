# Setup Databases


## Setup MySQL / MariaDB
If you already have your own MySQL database set up, you can skip this step and proceed with the next step to load TPC-H data.

In this tutorial, we will set up the MySQL database using Docker.


### Docker Installation

In this tutorial, we will launch MariaDB (i.e., open-source fork of MySQL), which is essentially equivalent to MySQL (at least for our tutorial), using Docker.

Docker is an open-source project for developers to create, deploy and run any application using containers.

You can download Docker [here](https://www.docker.com/community-edition#/download), and simply follow their instructions to install it.


### Start Your Database

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

## Setup Apache Spark

### Download / Install
Download a pre-compiled jar from [the official page](https://spark.apache.org/downloads.html). At the time of writing, the file for the latest version is `spark-2.3.1-bin-hadoop2.7.tgz`, which we will use in this article. For other versions, it should suffice to change the version number.

After the file is downloaded, move the file to your convenient location, then compress the file with the following command:

```bash
tar xzf spark-2.3.1-bin-hadoop2.7.tgz
```

### Start a Cluster

*(This section follows the steps in this [official guide](https://spark.apache.org/docs/latest/quick-start.html).)*

Move into the unarchived directory in the above step, then type the following command:

```bash
sbin/start-all.sh
```

The Spark cluster should be up and running. Its default status page is http://localhost:8080/


!!! warning "Connection refused error?"
    If you encounter the error when starting the Spark cluster, please refer to [this page](https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/port_22_connection_refused.html).
