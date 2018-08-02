# Interactive Querying

VerdictDB can give interactive answers for aggregate queries, which include `avg`, `sum`, `count`, `min`, and `max`. The SQL query syntax is almost identical to the standard.


## Syntax

```sql
SELECT [select_item, ] aggregate_expr [, aggregate_expr] [, ...]
FROM table_source
[WHERE predicate]
[GROUP BY group_expr]
[ORDER BY ordering_expr]
[LIMIT number];

select_item := regular_expr [AS alias];

table_source := base_table
              | base_table, table_source
              | base_table [INNER | LEFT | RIGHT] JOIN table_source ON condition
              | select_query;

group_expr := regular_expr | alias;

aggregate_expr := avg(regular_expr)
                | sum(regular_expr)
                | count(regular_expr)
                | min(regular_expr)
                | max(regular_expr);

regular_expr := unary_func(regular_expr)
              | binary_func(regular_expr, regular_expr)
              | regular_expr op regular_expr;

op := + | - | * | / | and | or;
```


## How does VerdictDB compute them?

VerdictDB applies different rules for different types of aggregate functions as follows. VerdictDB relies on the state-of-the-art techniques available in the literature.


### AVG, SUM, COUNT

VerdictDB's answers are always *unbiased estimators* of the true answers. For instance, if only 10% of the data (that amounts to the 10% uniform random sample of the data) is processed, the unbiased estimator for the count function is 10 times the answer computed on the 10% of the data. This logic becomes more complex as unbiased samples (within scrambles) are used for different types of aggregate functions. VerdictDB performs these computations (and proper scaling) all automatically.


### MIN, MAX

VerdictDB's answers to min and max functions are the min and max of the data that has been processed so far. For example, if 10% of the data was processed, then VerdictDB outputs min or max among those 10% data. Of course, the answers become more accurate as more data is processed and become exact when 100% data is processed. One possible concern is that the answers based on partial data may not be very accurate especially when a tiny fraction (e.g., 0.1%) of the data has been processed. To overcome this, VerdictDB processes outliers first. As a result, even the answers at the early stages are highly accurate.


### COUNT-DISTINCT

This is in preparation.
