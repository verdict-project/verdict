
VerdictDB can give interactive answers for aggregate queries, which include `avg`, `sum`, `count`, `min`, and `max`. The SQL query syntax is almost identical to the standard.


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
