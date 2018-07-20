package org.verdictdb.jdbc41;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

public class VerdictDatabaseMetaData implements java.sql.DatabaseMetaData {

  private DatabaseMetaData backend;
  
  public VerdictDatabaseMetaData(java.sql.DatabaseMetaData backend) {
    this.backend = backend;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return backend.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return backend.isWrapperFor(iface);
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return backend.allProceduresAreCallable();
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return backend.allTablesAreSelectable();
  }

  @Override
  public String getURL() throws SQLException {
    return backend.getURL();
  }

  @Override
  public String getUserName() throws SQLException {
    return backend.getUserName();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return backend.isReadOnly();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return backend.nullsAreSortedHigh();
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return backend.nullsAreSortedLow();
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return backend.nullsAreSortedAtStart();
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return backend.nullsAreSortedAtEnd();
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return backend.getDatabaseProductName();
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return backend.getDatabaseProductVersion();
  }

  @Override
  public String getDriverName() throws SQLException {
    return backend.getDriverName();
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return backend.getDriverVersion();
  }

  @Override
  public int getDriverMajorVersion() {
    return backend.getDriverMajorVersion();
  }

  @Override
  public int getDriverMinorVersion() {
    return backend.getDriverMinorVersion();
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return backend.usesLocalFiles();
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return backend.usesLocalFilePerTable();
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return backend.supportsMixedCaseIdentifiers();
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return backend.storesUpperCaseIdentifiers();
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return backend.storesLowerCaseIdentifiers();
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return backend.storesMixedCaseIdentifiers();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return backend.supportsMixedCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return backend.storesUpperCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return backend.storesLowerCaseQuotedIdentifiers();
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return backend.storesMixedCaseQuotedIdentifiers();
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return backend.getIdentifierQuoteString();
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return backend.getSQLKeywords();
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return backend.getNumericFunctions();
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return backend.getStringFunctions();
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return backend.getSystemFunctions();
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return backend.getTimeDateFunctions();
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return backend.getSearchStringEscape();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return backend.getExtraNameCharacters();
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return backend.supportsAlterTableWithAddColumn();
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return backend.supportsAlterTableWithDropColumn();
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return backend.supportsColumnAliasing();
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return backend.nullPlusNonNullIsNull();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return backend.supportsConvert();
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return backend.supportsConvert(fromType, toType);
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return backend.supportsTableCorrelationNames();
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return backend.supportsDifferentTableCorrelationNames();
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return backend.supportsExpressionsInOrderBy();
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return backend.supportsOrderByUnrelated();
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return backend.supportsGroupBy();
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return backend.supportsGroupByUnrelated();
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return backend.supportsGroupByBeyondSelect();
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return backend.supportsLikeEscapeClause();
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return backend.supportsMultipleResultSets();
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return backend.supportsMultipleTransactions();
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return backend.supportsNonNullableColumns();
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return backend.supportsMinimumSQLGrammar();
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return backend.supportsCoreSQLGrammar();
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return backend.supportsExtendedSQLGrammar() ;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return backend.supportsANSI92EntryLevelSQL();
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return backend.supportsANSI92IntermediateSQL();
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return backend.supportsANSI92FullSQL();
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return backend.supportsIntegrityEnhancementFacility();
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return backend.supportsOuterJoins();
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return backend.supportsFullOuterJoins();
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return backend.supportsLimitedOuterJoins();
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return backend.getSchemaTerm();
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return backend.getProcedureTerm();
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return backend.getCatalogTerm();
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return backend.isCatalogAtStart();
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return backend.getCatalogSeparator();
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return backend.supportsSchemasInDataManipulation();
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return backend.supportsSchemasInProcedureCalls();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return backend.supportsSchemasInTableDefinitions();
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return backend.supportsSchemasInIndexDefinitions();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return backend.supportsSchemasInPrivilegeDefinitions();
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return backend.supportsCatalogsInDataManipulation();
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return backend.supportsCatalogsInProcedureCalls();
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return backend.supportsCatalogsInTableDefinitions();
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return backend.supportsCatalogsInIndexDefinitions();
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return backend.supportsCatalogsInPrivilegeDefinitions();
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return backend.supportsPositionedDelete();
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return backend.supportsPositionedUpdate();
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return backend.supportsSelectForUpdate();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return backend.supportsStoredProcedures();
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return backend.supportsSubqueriesInComparisons();
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return backend.supportsSubqueriesInExists();
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return backend.supportsSubqueriesInIns();
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return backend.supportsSubqueriesInQuantifieds();
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return backend.supportsCorrelatedSubqueries();
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return backend.supportsUnion();
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return backend.supportsUnionAll();
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return backend.supportsOpenCursorsAcrossCommit();
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return backend.supportsOpenCursorsAcrossRollback();
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return backend.supportsOpenStatementsAcrossCommit();
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return backend.supportsOpenStatementsAcrossRollback();
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return backend.getMaxBinaryLiteralLength();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return backend.getMaxCharLiteralLength();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return backend.getMaxColumnNameLength();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return backend.getMaxColumnsInGroupBy();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return backend.getMaxColumnsInIndex();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return backend.getMaxColumnsInOrderBy();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return backend.getMaxColumnsInSelect();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return backend.getMaxColumnsInTable();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return backend.getMaxConnections();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return backend.getMaxCursorNameLength();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return backend.getMaxIndexLength();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return backend.getMaxSchemaNameLength();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return backend.getMaxProcedureNameLength();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return backend.getMaxCatalogNameLength();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return backend.getMaxRowSize();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return backend.doesMaxRowSizeIncludeBlobs();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return backend.getMaxStatementLength();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return backend.getMaxStatements();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return backend.getMaxTableNameLength();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return backend.getMaxTablesInSelect();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return backend.getMaxUserNameLength();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return backend.getDefaultTransactionIsolation();
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return backend.supportsTransactions();
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return backend.supportsTransactionIsolationLevel(level);
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return backend.supportsDataDefinitionAndDataManipulationTransactions();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return backend.supportsDataManipulationTransactionsOnly();
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return backend.dataDefinitionCausesTransactionCommit();
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return backend.dataDefinitionIgnoredInTransactions();
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return backend.getProcedures(catalog, schemaPattern, procedureNamePattern);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
      String columnNamePattern) throws SQLException {
    return backend.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    return backend.getTables(catalog, schemaPattern, tableNamePattern, types);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return backend.getSchemas();
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return backend.getCatalogs();
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return backend.getTableTypes();
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return backend.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
      throws SQLException {
    return backend.getColumnPrivileges(catalog, schema, table, columnNamePattern);
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return backend.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
      boolean nullable) throws SQLException {
    return backend.getBestRowIdentifier(catalog, schema, table, scope, nullable);
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return backend.getVersionColumns(catalog, schema, table);
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return backend.getPrimaryKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return backend.getImportedKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return backend.getExportedKeys(catalog, schema, table);
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return backend.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog,
        foreignSchema, foreignTable);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    return backend.getTypeInfo();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
      boolean approximate) throws SQLException {
    return backend.getIndexInfo(catalog, schema, table, unique, approximate);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return backend.supportsResultSetType(type);
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return backend.supportsResultSetConcurrency(type, concurrency);
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return backend.ownUpdatesAreVisible(type);
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return backend.ownUpdatesAreVisible(type);
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return backend.ownUpdatesAreVisible(type);
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return backend.othersUpdatesAreVisible(type);
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return backend.othersDeletesAreVisible(type);
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return backend.othersInsertsAreVisible(type);
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return backend.updatesAreDetected(type);
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return backend.deletesAreDetected(type);
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return backend.insertsAreDetected(type);
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return backend.supportsBatchUpdates();
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    return backend.getUDTs(catalog, schemaPattern, typeNamePattern, types);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return backend.getConnection();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return backend.supportsSavepoints();
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return backend.supportsNamedParameters();
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return backend.supportsMultipleOpenResults();
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return backend.supportsGetGeneratedKeys();
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    return backend.getSuperTypes(catalog, schemaPattern, typeNamePattern);
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return backend.getSuperTables(catalog, schemaPattern, tableNamePattern);
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    return backend.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return backend.supportsResultSetHoldability(holdability);
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return backend.getResultSetHoldability();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return backend.getDatabaseMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return backend.getDatabaseMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return backend.getJDBCMajorVersion();
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return backend.getJDBCMinorVersion();
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return backend.getSQLStateType();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return backend.locatorsUpdateCopy();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return backend.supportsStatementPooling();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return backend.getRowIdLifetime();
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return backend.getSchemas(catalog, schemaPattern);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return backend.supportsStoredFunctionsUsingCallSyntax();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return backend.autoCommitFailureClosesAllResultSets();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return backend.getClientInfoProperties();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    return backend.getFunctions(catalog, schemaPattern, functionNamePattern);
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
      String columnNamePattern) throws SQLException {
    return backend.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return backend.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return backend.generatedKeyAlwaysReturned();
  }
  
  

}
