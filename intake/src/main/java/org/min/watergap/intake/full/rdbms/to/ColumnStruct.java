package org.min.watergap.intake.full.rdbms.to;

import org.min.watergap.common.annotation.ResultSetMapping;

/**
 * jdbc 获取列信息
 *
 * @Create by metaX.h on 2021/11/19 23:08
 */
public class ColumnStruct extends BaseStruct {

    /**
     * TABLE_CAT String => table catalog (may be null)
     */
    @ResultSetMapping("TABLE_CAT")
    private String tableCatalog;

    /**
     * TABLE_SCHEM String => table schema (may be null)
     */
    @ResultSetMapping("TABLE_SCHEM")
    private String tableSchema;

    /**
     * TABLE_NAME String => table name
     */
    @ResultSetMapping("TABLE_NAME")
    private String tableName;

    /**
     * COLUMN_NAME String => column name
     */
    @ResultSetMapping("COLUMN_NAME")
    private String columnName;

    /**
     * DATA_TYPE int => SQL type from java.sql.Types
     */
    @ResultSetMapping("DATA_TYPE")
    private int dataType;

    /**
     * TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
     */
    @ResultSetMapping("TYPE_NAME")
    private String typeName;

    /**
     * COLUMN_SIZE int => column size.
     */
    @ResultSetMapping("COLUMN_SIZE")
    private int columnSize;

    /**
     * BUFFER_LENGTH is not used.
     */
    @ResultSetMapping("BUFFER_LENGTH")
    private int bufferLength;

    /**
     * DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
     */
    @ResultSetMapping("DECIMAL_DIGITS")
    private int decimalDigits;

    /**
     * NUM_PREC_RADIX int => Radix (typically either 10 or 2)
     */
    @ResultSetMapping("NUM_PREC_RADIX")
    private int numPrecRadix;

    /**
     * NULLABLE int => is NULL allowed.
     * columnNoNulls - might not allow NULL values
     * columnNullable - definitely allows NULL values
     * columnNullableUnknown - nullability unknown
     */
    @ResultSetMapping("NULLABLE")
    private int nullable;

    /**
     * REMARKS String => comment describing column (may be null)
     */
    @ResultSetMapping("REMARKS")
    private String remarks;

    /**
     * COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
     */
    @ResultSetMapping("COLUMN_DEF")
    private String columnDef;

    /**
     * SQL_DATA_TYPE int => unused
     */
    @ResultSetMapping("SQL_DATA_TYPE")
    private int sqlDataType;

    /**
     * SQL_DATETIME_SUB int => unused
     */
    @ResultSetMapping("SQL_DATETIME_SUB")
    private int sqlDatetimeSub;

    /**
     * CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
     */
    @ResultSetMapping("CHAR_OCTET_LENGTH")
    private int charOctetLength;

    /**
     * ORDINAL_POSITION int => index of column in table (starting at 1)
     */
    @ResultSetMapping("ORDINAL_POSITION")
    private int ordinalPosition;

    /**
     * IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
     * YES --- if the column can include NULLs
     * NO --- if the column cannot include NULLs
     * empty string --- if the nullability for the column is unknown
     */
    @ResultSetMapping("IS_NULLABLE")
    private String isNullable;

    /**
     * SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
     */
    @ResultSetMapping("SCOPE_CATALOG")
    private String scopeCatalog;

    /**
     * SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
     */
    @ResultSetMapping("SCOPE_SCHEMA")
    private String scopeSchema;

    /**
     * SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
     */
    @ResultSetMapping("SCOPE_TABLE")
    private String scopeTable;

    /**
     * SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
     */
    @ResultSetMapping("SOURCE_DATA_TYPE")
    private short sourceDataType;

    /**
     * IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
     * YES --- if the column is auto incremented
     * NO --- if the column is not auto incremented
     * empty string --- if it cannot be determined whether the column is auto incremented
     */
    @ResultSetMapping("IS_AUTOINCREMENT")
    private String isAutoincrement;

    /**
     * IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
     * YES --- if this a generated column
     * NO --- if this not a generated column
     * empty string --- if it cannot be determined whether this is a generated column
     */
    @ResultSetMapping("IS_GENERATEDCOLUMN")
    private String isGeneratedcolumn;

    public String getTableCatalog() {
        return tableCatalog;
    }

    public void setTableCatalog(String tableCatalog) {
        this.tableCatalog = tableCatalog;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getDataType() {
        return dataType;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public int getBufferLength() {
        return bufferLength;
    }

    public void setBufferLength(int bufferLength) {
        this.bufferLength = bufferLength;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public void setDecimalDigits(int decimalDigits) {
        this.decimalDigits = decimalDigits;
    }

    public int getNumPrecRadix() {
        return numPrecRadix;
    }

    public void setNumPrecRadix(int numPrecRadix) {
        this.numPrecRadix = numPrecRadix;
    }

    public int getNullable() {
        return nullable;
    }

    public void setNullable(int nullable) {
        this.nullable = nullable;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public String getColumnDef() {
        return columnDef;
    }

    public void setColumnDef(String columnDef) {
        this.columnDef = columnDef;
    }

    public int getSqlDataType() {
        return sqlDataType;
    }

    public void setSqlDataType(int sqlDataType) {
        this.sqlDataType = sqlDataType;
    }

    public int getSqlDatetimeSub() {
        return sqlDatetimeSub;
    }

    public void setSqlDatetimeSub(int sqlDatetimeSub) {
        this.sqlDatetimeSub = sqlDatetimeSub;
    }

    public int getCharOctetLength() {
        return charOctetLength;
    }

    public void setCharOctetLength(int charOctetLength) {
        this.charOctetLength = charOctetLength;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public String getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(String isNullable) {
        this.isNullable = isNullable;
    }

    public String getScopeCatalog() {
        return scopeCatalog;
    }

    public void setScopeCatalog(String scopeCatalog) {
        this.scopeCatalog = scopeCatalog;
    }

    public String getScopeSchema() {
        return scopeSchema;
    }

    public void setScopeSchema(String scopeSchema) {
        this.scopeSchema = scopeSchema;
    }

    public String getScopeTable() {
        return scopeTable;
    }

    public void setScopeTable(String scopeTable) {
        this.scopeTable = scopeTable;
    }

    public short getSourceDataType() {
        return sourceDataType;
    }

    public void setSourceDataType(short sourceDataType) {
        this.sourceDataType = sourceDataType;
    }

    public String getIsAutoincrement() {
        return isAutoincrement;
    }

    public void setIsAutoincrement(String isAutoincrement) {
        this.isAutoincrement = isAutoincrement;
    }

    public String getIsGeneratedcolumn() {
        return isGeneratedcolumn;
    }

    public void setIsGeneratedcolumn(String isGeneratedcolumn) {
        this.isGeneratedcolumn = isGeneratedcolumn;
    }

}
