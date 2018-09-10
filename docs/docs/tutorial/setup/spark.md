# Setup Apache Spark

## Download / Install

Download a pre-compiled jar from [the official page](https://spark.apache.org/downloads.html). At the time of writing, the file for the latest version is `spark-2.3.1-bin-hadoop2.7.tgz`, which we will use in this article. For other versions, it should suffice to change the version number.

After the file is downloaded, move the file to your convenient location, then compress the file with the following command:

```bash
tar xzf spark-2.3.1-bin-hadoop2.7.tgz
```

## Start a Cluster

*(This section follows the steps in this [official guide](https://spark.apache.org/docs/latest/quick-start.html).)*

Move into the unarchived directory in the above step, then type the following command:

```bash
sbin/start-all.sh
```

The Spark cluster should be up and running. Its default status page is http://localhost:8080/


!!! warning "Connection refused error?"
    If you encounter the error when starting the Spark cluster, please refer to [this page](https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/port_22_connection_refused.html).

