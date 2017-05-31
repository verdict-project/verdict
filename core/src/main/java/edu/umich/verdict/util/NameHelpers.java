package edu.umich.verdict.util;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;

public class NameHelpers {
	
	public static int viewNameId = 0;
	
	
	/**
	 * Returns an effective schema name of a table name specified in a query. For instance, given "default.products",
	 * this method returns "default". 
	 * @param originalTableName
	 * @return
	 */
	public static Optional<String> schemaOfTableName(Optional<String> currentSchema, String originalTableName) {
		String[] tokens = originalTableName.split("\\.");
		if (tokens.length > 1) {
			return Optional.fromNullable(tokens[0]);
		}
		else {
			return currentSchema;
		}
	}

	/**
	 * Returns an effective schema name of a table name specified in a query. For instance, given "default.products",
	 * this method returns "products".
	 * @param originalTableName
	 * @return
	 */
	public static String tableNameOfTableName(String originalTableName) {
		String[] tokens = originalTableName.split("\\.");
		if (tokens.length > 1) {
			return tokens[1];
		}
		else {
			return originalTableName;
		}
	}
	
	public static String attributeNameOfAttributeName(String originalAttrName) {
		String[] tokens = originalAttrName.split("\\.");
		if (tokens.length > 1) {
			return tokens[1];
		}
		else {
			return originalAttrName;
		}
	}
	
	/**
	 * Returns a unique name for a table (including its schema name).
	 * @param originalTableName
	 * @return
	 */
	public static String fullTableName(Optional<String> currentSchema, String originalTableName) {
		Optional<String> schema = schemaOfTableName(currentSchema, originalTableName);
		if (!schema.isPresent()) return tableNameOfTableName(originalTableName); 
		else 				 return  schema.get() + "." + tableNameOfTableName(originalTableName);
	}
	
}
