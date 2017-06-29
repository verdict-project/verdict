.. _veeline:

****************************************
Connecting to Databases using Veeline
****************************************

This document describes how to connect to Apache Hive, Apache (incubator) Impala, and MySQL using
Verdict's command-line interface :code:`veeline`.


Connecting to Hive
=====================

Verdict connects to Hive using the JDBC driver to `HiveServer2
<https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients>`_, which is the recommended
way of using Hive. Download and place the
`Hive JDBC driver jar file <http://www.mvnrepository.com/artifact/org.apache.hive/hive-jdbc>`_ in
:code:`libs` directory. Verdict source code includes `Hive JDBC Connector 2.5.4 for Cloudera
<https://www.cloudera.com/downloads/connectors/hive/jdbc/2-5-4.html>`_ by default. Type the
following command to connect to Hive using veeline.

.. code-block:: bash

    bash$ veeline/bin/veeline -h hive2://<host addrses>[:<port>] [-u <username>] [-p <password>]

If port is omitted, the default port (10000) is used. If username and password are omitted,
Verdict does not supply a user name and password when making a connection to Hive.


Connecting to Impala
=====================

Verdict connects to Impala using the `Cloudera JDBC Connector
<https://www.cloudera.com/documentation/enterprise/5-9-x/topics/impala_jdbc.html#jdbc_setup>`_,
which is the recommended way of connecting to Impala. Verdict source code includes the driver by
default. Type the following command to connect to Hive using veeline.

.. code-block:: bash

    bash$ veeline/bin/veeline -h impala://<host addrses>[:<port>] [-u <username>] [-p <password>]

If port is omitted, the default port (21050) is used. If username and password are omitted,
Verdict does not supply a user name and password when making a connection to Impala.


Connecting to MySQL
=====================

Verdict connects to MySQL using the `JDBC driver for MySQL
<https://dev.mysql.com/downloads/connector/j/>`_. The driver is also called MySQL Connector/J.
Verdict source code includes Connector/J 5.1.42 by default.  Type the following command to connect
to MySQL using veeline.

.. code-block:: bash

    bash$ veeline/bin/veeline -h mysql://<host addrses>[:<port>] [-u <username>] [-p <password>]

If port is omitted, the default port (3306) is used. MySQL always requires a user name; so it must
be specified. The password may be omitted; then, Verdict does not supply the password when
making a connection to MySQL.


Kerberos Authentication
=========================

Verdict supports Kerberos authentication for Hive and Impala. This feature was tested for Hive and
Impala included in a Cloudera distribution. First, obtain a valid Kerberos ticket using
:code:`kinit` command. Then, type the following command (for hive):

.. code-block:: bash

    bash$ veeline/bin/veeline -h hive2://<host addrses>[:<port>]?principal=hive/HiveServer2Host@YOUR-REALM.COM

The command is same for Impala except for the specifying "impala" in place of "hive2".


****************************************
After Connection is Made
****************************************

A successful connection to a database displays the following message.

.. code-block:: bash

    bash$ veeline/bin/veeline -h mysql://localhost -u verdict -p verdict
    DEBUG  2017-06-29 11:53:38,273 - [VerdictConnection] connection properties: user = verdict
    DEBUG  2017-06-29 11:53:38,273 - [VerdictConnection] connection properties: password = verdict
    DEBUG  2017-06-29 11:53:38,281 - [DbmsMySQL] JDBC connection string: jdbc:mysql://localhost:3306?user=verdict&password=verdict
    Thu Jun 29 11:53:38 EDT 2017 WARN: Establishing SSL connection without server's identity verification is not recommended. According to MySQL 5.5.45+, 5.6.26+ and 5.7.6+ requirements SSL connection must be established by default if explicit option isn't set. For compliance with existing applications not using SSL the verifyServerCertificate property is set to 'false'. You need either to explicitly disable SSL by setting useSSL=false, or set useSSL=true and provide truststore for server certificate verification.
    INFO   2017-06-29 11:53:38,695 - Connected to database: mysql//localhost:3306
    veeline version 0.2
    verdict:MySQL> 


Changing Log Level
==========================

Verdict's veeline uses log4j library for generating and recording logs. To change to log level (and to suppress the
above DEBUG messages), one can change the configuration in :code:`veeline/conf/log4j.properties` or
type the following command to change the log level.

.. code-block:: bash

    verdict:MySQL> set log4j.loglevel="warn";
    DEBUG  2017-06-29 11:58:03,251 - [VerdictStatement] execute() called with: set log4j.loglevel="warn"
    DEBUG  2017-06-29 11:58:03,254 - [VerdictContext] An input query:
    DEBUG  2017-06-29 11:58:03,254 - [VerdictContext]   set log4j.loglevel="warn"
    DEBUG  2017-06-29 11:58:03,254 - [Class] [3] A query type: CONFIG
    +-----------------+-------------+
    |    conf_key     | conf_value  |
    +-----------------+-------------+
    | log4j.loglevel  | warn        |
    +-----------------+-------------+
    1 row selected (0.005 seconds)

You can see that issuing the same command :code:`set log4j.loglevel="warn";` does not display the
DEBUG messages any more.

.. code-block:: bash

    verdict:MySQL> set log4j.loglevel="warn";
    +-----------------+-------------+
    |    conf_key     | conf_value  |
    +-----------------+-------------+
    | log4j.loglevel  | warn        |
    +-----------------+-------------+
    1 row selected (0.005 seconds)


Browing Datatabse and Querying
==================================

Now you can issue any Verdict-supported commands in veeline. Please see :ref:`features` for more
information.


