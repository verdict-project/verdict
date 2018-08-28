# VerdictDB Documentation

## Introduction

VerdictDB is a thin, platform-independent, interactive analytics library that works on top of your existing (or backend) database system (e.g., MySQL, PostgreSQL, Redshift, etc.). For platform-independence, VerdictDB makes all communications with the backend database in SQL. For interactive querying, VerdictDB has the ability to properly adjust the output values even when only a fraction of the data is processed. The initial outputs are already pretty accurate, and they can become more accurate (naturally) as more data is processed. These partial (and incremental) data processing are performed automatically by VerdictDB using query rewriting. The connections to VerdictDB can be made in Java.

*Note: Python interface will be soon added.*


## Contents

1. Getting Started
    - [Quickstart](/getting_started/quickstart.md)
    - [Install / Download](/getting_started/install.md)
    - [Connecting to Databases](/getting_started/connection.md)
    - [Scrambling](/getting_started/scrambling.md)
    - [Interactive Querying](/getting_started/querying.md)
1. How it works
    - [Basic Idea](/how_it_works/basic_idea.md)
    - [Architecture](/how_it_works/architecture.md)
    - [Query Processing](/how_it_works/query_processing.md)
1. Tutorial
    - [Setting up TPC-H data](/tutorial/tpch/)


## License

VerdictDB is under the Apache License, thus is completely free for both commercial and non-commercial purposes.