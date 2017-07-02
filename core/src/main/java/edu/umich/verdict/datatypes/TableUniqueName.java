package edu.umich.verdict.datatypes;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.util.NameHelpers;

public class TableUniqueName implements Comparable {
	public final String schemaName;
	public final String tableName;
	
	public TableUniqueName(String schemaName, String tableName) {
		this.schemaName = (schemaName != null)? schemaName.toLowerCase() : schemaName;
		this.tableName = (tableName != null)? tableName.toLowerCase() : tableName;
	}
	
	public static TableUniqueName uname(String schemaName, String tableName) {
		return new TableUniqueName(schemaName, tableName);
	}
	
	public static TableUniqueName uname(VerdictContext vc, String tableName) {
		return new TableUniqueName(NameHelpers.schemaOfTableName(vc.getCurrentSchema(), tableName).get(),
								   NameHelpers.tableNameOfTableName(tableName));
	}
	
	public String fullyQuantifiedName() {
		return fullyQuantifiedName(schemaName, tableName);
	}
	
	public static String fullyQuantifiedName(String schema, String table) {
		return NameHelpers.fullTableName(Optional.fromNullable(schema), table);
	}
	
	@Override
	public int hashCode() {
		return schemaName.hashCode() + tableName.hashCode();
	}
	
	@Override
	public boolean equals(Object another) {
		if (another instanceof TableUniqueName) {
			TableUniqueName t = (TableUniqueName) another;
			return schemaName.equals(t.schemaName) && tableName.equals(t.tableName);			
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return fullyQuantifiedName(schemaName, tableName);
	}

	@Override
	public int compareTo(Object another) {
		if (another instanceof TableUniqueName) {
			TableUniqueName t = (TableUniqueName) another;
			if (this.schemaName.compareTo(t.schemaName) == 0) {
				return this.tableName.compareTo(t.tableName);
			} else {
				return this.schemaName.compareTo(t.schemaName);
			}
		} else {
			return 0;
		}
	}
}