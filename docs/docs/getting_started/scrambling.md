# Creating Scrambles

## Syntax

```sql
CREATE SCRAMBLE newSchema.newTable FROM originalSchema.originalTable;
```

Note:

1. ``newSchema` may be identical to `originalSchema`.
1. `newTable` must be different from `originalTable`.
1. The user requires the write privilege for the `newSchema` schema and the read privilege for the `originalSchema.originalTable`.
1. The schema and table names can be quoted either using the double-quote (") or the backtick (`).
1. The schema names, table names, column names, etc. in the queries issued by VerdictDB to the backend database are always quoted.


## What is a Scramble?

For VerdictDB's interactive query processing, a special table called a *scramble* must be created for an original table. The queries including scrambles are automatically rewritten by VerdictDB in a way to enable interactive querying. The queries including the original table(s) are also first rewritten to use its/their corresponding scrambles; then, VerdictDB applies to the same mechanism to enable interactive querying for those queries. If no scrambles have been created for a table, no query rewritting is performed.

Every time a scramble is created, VerdictDB stores the information in its own metadata schema (`verdictdbmetadata` by default). The relationship between the original tables and scrambles are recognized using this metadata.