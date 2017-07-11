package edu.umich.verdict.datatypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ColumnName {
	
	private TableUniqueName tableSource;	// an original table source
	
	private String tableName;				// could be an alias
	
	private String columnName;

	public ColumnName(TableUniqueName tableSource, String tableName, String columnName) {
		this.tableSource = tableSource;
		this.tableName = tableName;
		this.columnName = columnName;
	}
	
	/**
	 * 
	 * @param columnName a possibly full column name including table name and column name
	 * @return
	 */
	private static Pair<String, String> parseTableAndColumnName(String columnName) {
		String[] tokens = columnName.split("\\.");
		if (tokens.length > 1) {
			return Pair.of(tokens[tokens.length-2], tokens[tokens.length-1]);
		} else {
			return Pair.of(null, tokens[tokens.length-1]);
		}
	}
	
//	private static Set<String> columnsToTables(List<TableUniqueName> tableSources) {
//		Set<String> schemaNames = new HashSet<String>();
//		for (TableUniqueName t : tableSources) {
//			schemaNames.add(t.schemaName);
//		}
//		return schemaNames;
//	}
	
	/**
	 * 
	 * @param vc
	 * @param tableName
	 * @param columnName
	 * @param tableSources 
	 * @return A pair of table name and its alias
	 * @throws VerdictException 
	 */
	private static Pair<TableUniqueName, String> effectiveTableSourceForColumn(
			VerdictJDBCContext vc, String tableName, String columnName, List<Pair<TableUniqueName, String>> tableSources) throws VerdictException {
		if (tableName != null) {
			for (Pair<TableUniqueName, String> e : tableSources) {
				if (tableName.equals(e.getLeft().getTableName()) || tableName.equals(e.getRight())) {
					return e;
				}
			}
			throw new VerdictException(String.format("The table name, %s, cannot be resolved.", tableName));
		} else {
			List<Pair<TableUniqueName, String>> allPossiblePairs = new ArrayList<Pair<TableUniqueName, String>>();
			for (Pair<TableUniqueName, String> e : tableSources) {
				Set<String> columns = vc.getMeta().getColumns(e.getLeft());
				for (String c : columns) {
					if (c.equals(columnName)) {
						allPossiblePairs.add(e);
					}
				}
			}
			if (allPossiblePairs.size() == 1) {
				return allPossiblePairs.get(0);
			} else if (allPossiblePairs.size() > 1) {
				throw new VerdictException(String.format("The column name, %s, is ambiguous.", columnName));
			} else {
				throw new VerdictException(String.format("The column name, %s, cannot be resolved.", columnName));
			}
		}
	}
	
	/**
	 * 
	 * @param vc
	 * @param tableSources Pairs of a table source name and its alias 
	 * @param columnName
	 * @return
	 * @throws VerdictException 
	 */
	public static ColumnName uname(VerdictJDBCContext vc, List<Pair<TableUniqueName, String>> tableSources, String columnName) throws VerdictException {
		Pair<String, String> tc = parseTableAndColumnName(columnName);
		Pair<TableUniqueName, String> tableNameAndAlias = effectiveTableSourceForColumn(vc, tc.getLeft(), tc.getRight(), tableSources);
		String tableName = (tableNameAndAlias.getRight() == null)? tableNameAndAlias.getLeft().getTableName() : tableNameAndAlias.getRight(); 
		return new ColumnName(tableNameAndAlias.getLeft(), tableName, tc.getRight());
	}
	
	public TableUniqueName getTableSource() {
		return tableSource;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String localColumnName() {
		return columnName;
	}
	
	@Override
	public String toString() {
		return tableName.toString() + "." + columnName;
	}

}
