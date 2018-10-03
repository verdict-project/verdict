# Creating/Viewing Scrambles

A scramble is a special table used by VerdictDB to speed up query processing. The information about the created scrambles is stored in its metadata table and is used at query time.


## Syntax for Creating Scrambles

```sql
CREATE SCRAMBLE newSchema.newTable FROM originalSchema.originalTable;
```

Note:

1. `newSchema` may be identical to `originalSchema`.
1. `newTable` must be different from `originalTable`.
1. The user requires the write privilege for the `newSchema` schema and the read privilege for the `originalSchema.originalTable`.
1. The schema and table names can be quoted either using the double-quote (") or the backtick (`).
1. The schema names, table names, column names, etc. in the queries issued by VerdictDB to the backend database are always quoted.

## Syntax for Viewing Scrambles

```sql
SHOW SCRAMBLES;
```

This query will print the list of all scrambled tables that have been built.