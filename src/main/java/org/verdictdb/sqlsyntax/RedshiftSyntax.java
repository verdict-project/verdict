package org.verdictdb.sqlsyntax;

public class RedshiftSyntax extends SqlSyntax {

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }


  @Override
  public String getQuoteString() {
    return "\"";
  }

  @Override
  public void dropTable(String schema, String tablename) {

  }

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public String randFunction() {
    return "random()";
  }

  @Override
  public String getSchemaCommand() {
    return "select schema_name from information_schema.schemata";
  }

  @Override
  public String getTableCommand(String schema) {
    return "SELECT table_name FROM information_schema.tables " + "WHERE table_schema = '" + schema +"'";
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "select column_name, data_type " +
        "from INFORMATION_SCHEMA.COLUMNS where table_name = '" + table + "' and table_schema = '" + schema + "'";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format(
        "SET search_path to '%s'; ", schema));
    sql.append(String.format(
        "SELECT \"column\", \"type\" FROM PG_TABLE_DEF " +
        "WHERE \"schemaname\" = '%s' and \"tablename\" = '%s' " +
          "and \"sortkey\" > 0 " +
        "ORDER BY \"sortkey\";",
        schema, table));
    return sql.toString();
//    return "select partattrs from pg_partitioned_table join pg_class on pg_class.relname='" + table + "' " +
//        "and pg_class.oid = pg_partitioned_table.partrelid join information_schema.tables " +
//        "on table_schema='" + schema + "' and table_name = '" + table + "'";
  }

  /**
   * Documentation about SORTKEY: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
   * 
   * If you do not specify any sort keys, the table is not sorted by default. You can
   * define a maximum of 400 COMPOUND SORTKEY columns or 8 INTERLEAVED SORTKEY
   * columns per table.
   * 
   * COMPOUND
   * Specifies that the data is sorted using a compound key made up of all of the
   * listed columns, in the order they are listed. A compound sort key is most useful
   * when a query scans rows according to the order of the sort columns. The
   * performance benefits of sorting with a compound key decrease when queries rely
   * on secondary sort columns. You can define a maximum of 400 COMPOUND SORTKEY
   * columns per table.
   * 
   * INTERLEAVED
   * Specifies that the data is sorted using an interleaved sort key. A maximum of
   * eight columns can be specified for an interleaved sort key.
   * 
   * An interleaved sort gives equal weight to each column, or subset of columns, in
   * the sort key, so queries do not depend on the order of the columns in the sort
   * key. When a query uses one or more secondary sort columns, interleaved sorting
   * significantly improves query performance. Interleaved sorting carries a small
   * overhead cost for data loading and vacuuming operations.
   * 
   * Important
   * 
   * Don’t use an interleaved sort key on columns with monotonically increasing
   * attributes, such as identity columns, dates, or timestamps.
   */
  @Override
  public String getPartitionByInCreateTable() {
    return "COMPOUND SORTKEY";
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    return true;
  }
  
}
