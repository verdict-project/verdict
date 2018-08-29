# VerdictDB Documentation

## Introduction

VerdictDB is a thin, platform-independent, interactive analytics library that works on top of your existing (or backend) database system (e.g., MySQL, PostgreSQL, Redshift, etc.). For platform-independence, VerdictDB makes all communications with the backend database in SQL. For interactive querying, VerdictDB intelligently **infers** the query answers based on the results processed on a part of the original data. Those inferred answers are highly accurate estimators of the exact answers. Furthermore, *even when only exact answers are needed*, VerdictDB can also be useful with its **streaming sql engine**. The streaming sql engine provides interactive-speed feedbacks in the process of computing exact answers. See [this page](/how_it_works/basics) if you want to know more about VerdictDB's internal mechanism.


## Contents

1. Getting Started
    - [Quickstart](/getting_started/quickstart)
    - [Install / Download](/getting_started/install)
    - [Connecting to Databases](/getting_started/connection)
    - [Scrambling](/getting_started/scrambling)
    - [Interactive Querying](/getting_started/querying)
    - [Result Structure](/getting_started/results)
1. How VerdictDB works
    - [Basics](/how_it_works/basics)
    - [Architecture](/how_it_works/architecture)
    - [Query Processing](/how_it_works/query_processing)
1. Tutorial
    - [Setting up MySQL database](/tutorial/mysql)
    - [Setting up TPC-H data](/tutorial/tpch)
    - [An example Java application](/tutorial/example)



## License

VerdictDB is under [the Apache License](https://www.apache.org/licenses/LICENSE-2.0), thus is completely free for both commercial and non-commercial purposes.