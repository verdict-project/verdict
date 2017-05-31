.. Verdict documentation master file, created by
   sphinx-quickstart on Mon May  8 17:39:08 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. title:: Verdict: Interactive Big Data Analytics

.. raw:: html

    <div class="verdict-intro">
        <p class="verdict-intro-text">
        <b>Verdict</b> is an approximate, big data analytics system.
        </p>
    </div>


.. raw:: html

    <div class="container-fluid intro-row">
        <div class="row">
            <div class="col-sm-1"></div>
            <div class="col-sm-5">
                <img class="intro-image" src="_static/images/index-html-impala.png" />
                <p class="text-center">Answer by <b>Impala</b> in 350 seconds</p>
            </div>
            <div class="col-sm-5">
                <img class="intro-image" src="_static/images/index-html-verdict.png" />
                <p class="text-center">Answer by <b>Verdict</b> in 2 seconds</p>
            </div>
            <div class="col-sm-1"></div>
        </div>
    </div>

|


200x faster by sacrificing only 1% accuracy
===========================================

Verdict can give you 99% accurate answers for your big data queries in a fraction of
the time needed for calculating exact answers. If your data is too big to
analyze in a couple of seconds, you will like Verdict.


No change to your database
===================================

Verdict is a middleware standing between your application and your database. You can
just issue the same queries as before and get approximate answers right away. Of
course, Verdict handles exact query processing too.


Runs on (almost) any database
===================================

Verdict can run on any database that supports standard SQL.
We already have drivers for Hive, Impala, and MySQL. We'll soon add drivers
for some other popular databases.


Easy of use
===================================

Verdict is a client-side library: no servers, no port configurations, no extra
user authentication, etc. Verdict simply issues rewritten SQL queries to your databases on behalf of
the client. The use of Verdict does not introduce any security breaches. Verdict relies on standard
protocols for communicating with the databases, which can be configured to be secure.

|

-----------------

How Verdict provides speedups?
===================================

Verdict is built upon the theories of approximate query processing (AQP) and our
novel architecture of AQP-as-a-middleware. Verdict's huge speedups are possible
because, even from a small fraction of the entire data, we can reliably estimate
many important statistics of the entire data. Verdict exploits that the values
of many aggregate functions that commonly appear in analytic queries can be expressed using those
statistics of the entire data, which can be estimated using samples.


Who develop Verdict?
===================================

Verdict is developed and maintained primarily by the database group at the
University of Michigan, Ann Arbor. The database group at the University of
Michigan, Ann Arbor is a leading research lab in the area of approximate query
processing and big data analytics.
Our research in approximate query processing has produced many academic research
papers (:ref:`academic`), and the knowledge we have gained from those
experiences is used for building Verdict.


.. **Code Documentation**
.. 
.. * `Core Documentation <javadoc/core/index.html>`_
.. * `JDBC Documentation <javadoc/jdbc/index.html>`_
.. Indices and tables
.. ==================
.. 
.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
