package edu.umich.verdict.datatypes;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class VerdictResultColumnMap {
	
	private Map<Integer, Integer> aggColumn2ErrorColumn;
	
	private Map<String, String> aliasToColumnLabel;
	
	private ResultSet rs;

	public VerdictResultColumnMap(
			Map<Integer, Integer> aggColumn2ErrorColumn,
			Map<String, String> aliasToColumnLabel,
			ResultSet rs) {
		this.aggColumn2ErrorColumn = aggColumn2ErrorColumn;
		this.aliasToColumnLabel = aliasToColumnLabel;
		this.rs = rs;
	}
	
	public String getColumnLabel(int columnIndex) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		String aliasLabel = meta.getColumnLabel(columnIndex);
		String baseName = aliasToColumnLabel.get(aliasLabel);
		
		for (Map.Entry<Integer, Integer> entry : aggColumn2ErrorColumn.entrySet()) {
			if (entry.getValue().equals(columnIndex)) {
				// if an error column, use the modified name
				baseName = "Error bound of " + baseName;
			}
		}

		return baseName;
	}

}
