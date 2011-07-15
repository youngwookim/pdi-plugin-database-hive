package com.nexr.pdi.plugin.database.hive;

import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;

@DatabaseMetaPlugin(type = "HADOOP HIVE", typeDescription = "Hadoop Hive")
public class HiveDatabaseMeta extends BaseDatabaseMeta implements
		DatabaseInterface {

	public String getDriverClass() {

		return "org.apache.hadoop.hive.jdbc.HiveDriver";
	}

	public String getURL(String hostname, String port, String databaseName)
			throws KettleDatabaseException {

		return "jdbc:hive://" + hostname + ":" + port + "/" + databaseName;
	}

	public String getAddColumnStatement(String tablename, ValueMetaInterface v,
			String tk, boolean use_autoinc, String pk, boolean semicolon) {
		return "ALTER TABLE " + tablename + " ADD "
				+ getFieldDefinition(v, tk, pk, use_autoinc, true, false);
	}

	public String getModifyColumnStatement(String tablename,
			ValueMetaInterface v, String tk, boolean use_autoinc, String pk,
			boolean semicolon) {
		return "ALTER TABLE " + tablename + " MODIFY "
				+ getFieldDefinition(v, tk, pk, use_autoinc, true, false);
	}

	public String[] getUsedLibraries() {
		return new String[] { "" };
	}

	@Override
	public int[] getAccessTypeList() {

		return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE };
	}

	@Override
	public int getDefaultDatabasePort() {
		if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
			return 10000;
		} else {
			return -1;
		}
	}
	
	public String getFieldDefinition(ValueMetaInterface v, String tk,
			String pk, boolean use_autoinc, boolean add_fieldname,
			boolean add_cr) {
		String retval = "";

		String fieldname = v.getName();
		int length = v.getLength();
		int precision = v.getPrecision();

		if (add_fieldname) {
			retval += fieldname + " ";
		}

		int type = v.getType();
		switch (type) {

		case ValueMetaInterface.TYPE_BOOLEAN:
			retval += "BOOLEAN";
			break;

		// Hive does not support DATE
		case ValueMetaInterface.TYPE_DATE:
			retval += "STRING";
			break;

		case ValueMetaInterface.TYPE_STRING:
			retval += "STRING";
			break;

		case ValueMetaInterface.TYPE_NUMBER:
		case ValueMetaInterface.TYPE_INTEGER:
		case ValueMetaInterface.TYPE_BIGNUMBER:
			// Integer values...
			if (precision == 0) {
				if (length > 9) {
					if (length < 19) {
						// can hold signed values between -9223372036854775808
						// and 9223372036854775807
						// 18 significant digits
						retval += "BIGINT";
					} else {
						retval += "FLOAT";
					}
				} else {
					retval += "INT";
				}
			}
			// Floating point values...
			else {
				if (length > 15) {
					retval += "FLOAT";
				} else {
					// A double-precision floating-point number is accurate to
					// approximately 15 decimal places.
					// http://mysql.mirrors-r-us.net/doc/refman/5.1/en/numeric-type-overview.html
					retval += "DOUBLE";
				}
			}

			break;
		}

		return retval;
	}

	@Override
	public boolean supportsAutoInc() {
		return false;
	}

	@Override
	public String getLimitClause(int nrRows) {

		return " LIMIT " + nrRows;
	}

	@Override
	public boolean isFetchSizeSupported() {

		return true;
	}

	@Override
	public boolean supportsTransactions() {

		return false;
	}

	@Override
	public boolean supportsSequences() {

		return false;
	}

	@Override
	public String[] getReservedWords() {

		return new String[] { "ADD", "ALL", "ALTER", "AND", "AS", "ASC", "ARRAY",
				"BEFORE", "BETWEEN", "BIGINT", "BY", "CASE", "COLUMN", "COLUMNS", "COMMENT", "COLLECTION",
				"CREATE", "CROSS", "CLUSTER", "CLUSTERED", "DATABASE", "DATABASES", "DESC", "DEFAULT",
				"DESCRIBE", "DISTRIBUTE", "DELIMITED", "DIRECTORY", "DISTINCT",
				"DOUBLE", "DROP", "EXISTS", "EXPLAIN", "EXTENDED", "EXTERNAL", "FLOAT",
				"FORMAT", "FROM", "FIELDS", "GRANT", "GROUP", "HAVING", "IF",
				"IN", "INDEX", "INNER", "INOUT", "INSENSITIVE", "INSERT", "ITEMS",
				"INT", "INTO", "IS", "ITERATE", "JOIN",
				"KEYS", "LEFT", "LIKE", "LIMIT", "LOAD", "LOCATION", "LOCK", "LINES",
				"MAP", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER",
				"OVERWRITE", "PARTITION", "PARTITIONED", "REGEXP", "ROW", "REDUCE", "RENAME", "REPLACE",
				"REVOKE", "RIGHT", "RLIKE", "SELECT", "SET", "SHOW", "SORT",
				"SMALLINT", "STRUCT", "STORED", "STRING", "TABLE",
				"TERMINATED", "TINYINT", "TRANSFORM", "THEN", "UNION", "USE",
				"USING", "WHEN", "WHERE" };
	}

	@Override
	public boolean supportsRepository() {

		return false;
	}

	@Override
	public boolean supportsViews() {

		return true;
	}

	@Override
	public boolean supportsSynonyms() {

		return false;
	}

	@Override
	public String getSQLQueryFields(String tableName) {

		return "SELECT * FROM " + tableName + " LIMIT 1";
	}

	@Override
	public boolean supportsBatchUpdates() {

		return false;
	}

	@Override
	public boolean supportsBooleanDataType() {

		return false;
	}

	@Override
	public String getSQLTableExists(String tablename) {

		return getSQLQueryFields(tablename);
	}

	@Override
	public boolean isExplorable() {

		return true;
	}

	@Override
	public String getXulOverlayFile() {

		return "comon";
	}

	@Override
	public String getSelectCountStatement(String tableName) {

		return "SELECT count(*) FROM " + tableName;
	}

	public String getExtraOptionsHelpText() {
		return "http://hive.apache.org/";
	}

}