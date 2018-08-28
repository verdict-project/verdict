# Basics

The idea is simple: by organizing data in a special way in advance, your queries can run faster.

This general idea has been widely-used in the various database systems, including modern distributed SQL-on-Hadoop systems. The examples are indexing, columnar format, compression, etc. Our system, VerdictDB, uses a new orthogonal approach, which we call *scrambling* (more details follow shortly).
Since our approach is orthogonal to the existing approaches (e.g., columnar formats and compressions), we can combine scrambling with other approaches as well;
in fact, VerdictDB also uses columnar format and compression (as well as scrambling) for maximum possible speedups whenever possible.

VerdictDB mainly focuses on speeding up *aggregate* queries, i.e., the queries including aggregate functions, such as `sum`, `count`, `avg`, and so on. Unlike lookup-style queries, these queries typically require extensive data scans; thus, query latencies can be very slow especially when the size of data is large. Besides aggregate queries, we envision to support all interesting pre-configurations of data that can be performed for speeding up query processing in a platform-independent manner.

Then, what is **scrambling** and how do queries run faster with it? We present conceptual ideas below. VerdictDB's deployment, user interface, and architecture are discussed on [this page](/how_it_works/architecture). More details about internal query processing logic is described on [this page](/how_it_works/query_processing).


## Scrambling

A scramble is a regular database table that consists of randomly shuffle tuples (of an original table) with an extra, augmented column named `verdidctdbblock`. Physically, a set of tuples associated with the same `verdidctdbblock` are clustered (using `partition` supported by most modern databases).

VerdictDB creates a scramble when the user issues a [create-scramble query](/getting_started/scrambling).


## How do queries run faster

To quickly produce the answers to aggregate queries, VerdictDB relies on a well-known statistical property called [the law of large numbers](https://en.wikipedia.org/wiki/Law_of_large_numbers). The law of large numbers indicate that many commonly-used statistics (e.g., mean) can be precisely estimated simply using a *sample*, a randomly chosen subset of the original data. Observe that a set of tuples associated with the `verdictdbblock` in a scramble amounts to a sample; thus, by processing one or just a few blocks, we can produce very accurate estimates of the **exact** answer, i.e., the answer you would get if the entire data is processed.

In some situations, however, the exact answers are still desirable and VerdictDB's estimated answers may not be directly useful. However, even to support the case, VerdictDB has a special mechanism called *streaming query engine*. Streaming query engine produces interactive feedback while the exact answer is being computed. With this mechanism, you can simply stop the execution in the middle if the exact answer you would get eventually is not something you initially imagined (e.g., wrong filtering conditions, wrong tables or columns specified, etc.). Find more details about the streaming query engine on [this page](/how_it_works/query_processing).
