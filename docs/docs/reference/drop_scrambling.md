# Dropping Scrambles

You can either drop 1) a specific scramble; or 2) all scrambles that belong to a single table.

## Syntax for Dropping a Specific Scramble

```sql
DROP SCRAMBLE scrambleSchema.scrambleTable ON originalSchema.originalTable;
```

This will remove the `scrambleSchema.scrambleTable` scramble built for `originalSchema.originalTable` and its metadata in VerdictDB.

## Syntax for Dropping All Scrambles for a Table

```sql
DROP ALL SCRAMBLE originalSchema.originalTable;
```

This will remove all scrambles built for `originalSchema.originalTable` and their metadata in VerdictDB.