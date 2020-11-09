package com.web.vo;

import org.apache.commons.lang.builder.ToStringBuilder;

public class ColumnInfo {
	private static String tableName;
	private static String columnName;
	private static Integer columnType;
	private static Integer columnDisplaySize;	
	
	public static String getTableName() {
		return tableName;
	}
	public static void setTableName(String tableName) {
		ColumnInfo.tableName = tableName;
	}
	public static String getColumnName() {
		return columnName;
	}
	public static void setColumnName(String columnName) {
		ColumnInfo.columnName = columnName;
	}
	public static Integer getColumnType() {
		return columnType;
	}
	public static void setColumnType(Integer columnType) {
		ColumnInfo.columnType = columnType;
	}
	public static Integer getColumnDisplaySize() {
		return columnDisplaySize;
	}
	public static void setColumnDisplaySize(Integer columnDisplaySize) {
		ColumnInfo.columnDisplaySize = columnDisplaySize;
	}
	public String toString() {
		return ToStringBuilder.reflectionToString(this).toString();
	}	
}
