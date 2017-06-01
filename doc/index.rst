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


**200x faster by sacrificing only 1% accuracy**
=================================================

Verdict can give you 99% accurate answers for your big data queries in a fraction of
the time needed for calculating exact answers. If your data is too big to
analyze in a couple of seconds, you will like Verdict.


**No change to your database**
===================================

Verdict is a middleware standing between your application and your database. You can
just issue the same queries as before and get approximate answers right away. Of
course, Verdict handles exact query processing too.


**Runs on (almost) any database**
===================================

Verdict can run on any database that supports standard SQL.
We already have drivers for Hive, Impala, and MySQL. We'll soon add drivers
for some other popular databases.


**Easy of use**
===================================

Verdict is a client-side library: no servers, no port configurations, no extra
user authentication, etc. Verdict simply issues rewritten SQL queries to your databases on behalf of
the client. The use of Verdict does not introduce any security breaches. Verdict relies on standard
protocols for communicating with the databases, which can be configured to be secure.

|

-------------------------

.. raw:: html

    <div class="container-fluid intro-row">
        <div class="row">
            <div class="col-sm-5">
                <h2><b>Techniques</b></h2>
                <p>
                Are you curious about how Verdict provides such large speedups?
                Find the techniques behind Verdict in our
                <a href="documentation/academic.html">research</a> page.
                </p>
            </div>
            <div class="col-sm-1"></div>
            <div class="col-sm-5">
                <h2><b>Getting Started</b></h2>
                <p>Verdict processes the standard SQL queries, and no
                installation is required.</p>
                <ol>
                <li><a href="download.html">Download</a> the latest release.</li>
                <li>Read the <a href="documentation/tutorial.html">Quick Start
                Guide</a>.</li>
                </ol>
            </div>
            <div class="col-sm-1"></div>
        </div>
    </div>



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
