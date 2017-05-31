
Overview
===================================

Verdict is a DBMS-agnostic library that provides approximate query processing
(AQP) capability to existing database engines, speeding up those engines
(including Spark SQL, Impala, Hive, etc.) by hundreds times.


Getting Started
===================================

Verdict supports the standard JDBC interface. You can simply download Verdict
jar file and load it in the same way as other JDBC drivers.

Download the jar file from this link.

Place the downloaded jar file in your convenient location, and add the path to
the jar file in your classpath::

    export CLASSPATH=<path-to-the-verdict-jar-file>:$CLASSPATH


Verdict on a Cluster
===================================


Options
===================================



API Docs
===================================

* `Core Documentation <javadoc/core/index.html>`_
* `JDBC Documentation <javadoc/jdbc/index.html>`_
