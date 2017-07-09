package edu.umich.verdict.util;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;

public class StringManupulations {
	
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
	
	public static String colNameOfColName(String originalColName) {
		String[] tokens = originalColName.split("\\.");
		if (tokens.length > 1) {
			return tokens[tokens.length-1];
		}
		else {
			return originalColName;
		}
	}
	
	public static String tabNameOfColName(String originalColName) {
		String[] tokens = originalColName.split("\\.");
		if (tokens.length > 1) {
			return tokens[tokens.length-2];
		} else {
			return "";
		}
	}
	
	public static TableUniqueName tabUniqueNameOfColName(VerdictJDBCContext vc, String originalColName) {
		String[] tokens = originalColName.split("\\.");
		if (tokens.length > 2) {
			return TableUniqueName.uname(tokens[tokens.length-3], tokens[tokens.length-2]);
		} else if (tokens.length > 1) {
			return TableUniqueName.uname(vc, tokens[tokens.length-2]);
		} else {
			return null;
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
	
	public static VerdictSQLParser parserOf(String text) {
		VerdictSQLLexer l = new VerdictSQLLexer(new ANTLRInputStream(text));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		return p;
	}
}
