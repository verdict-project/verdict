# Appending Scrambles

You can append new data into existing scrambles.

## Syntax for Appending Data to an Existing Scramble

```sql
[INSERT|APPEND] SCRAMBLE existingSchema.existingTable WHERE condition
```

Note:

1. VerdictDB obtains new data from the original table used to create the scramble `existingSchema.existingTable` that satisfty given `condition` (e.g., year=2019).
1. The same method (e.g., uniform or hash) used to create the existing scramble will be used to append new data.
1. It is user's responsibility to make sure that (s)he does not append duplicate data into existing schema.

## Example of Appending Data to an Existing Scramble

For example, if you want to add new data collected in the year of 2019 into existing scramble `ourSchema.scrmableTable` (assuming the scramble only contains data up to 2018), you may issue the following query to VerdictDB:

```sql
APPEND SCRAMBLE ourSchema.scrambleTable WHERE year=2019;
```