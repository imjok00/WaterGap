package org.min.watergap.outfall.convertor;

import org.min.watergap.common.utils.StringUtils;
import org.min.watergap.piping.translator.impl.BasePipingData;
import org.min.watergap.piping.translator.impl.FullTableDataBasePipingData;
import org.min.watergap.piping.translator.impl.SchemaStructBasePipingData;
import org.min.watergap.piping.translator.impl.TableStructBasePipingData;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlStructConvertor implements StructConvertor {

    // (M) 类型
    public static final List<String> LENGTH_NEED_IDENT = Arrays.asList("BIT", "TINYINT", "SMALLINT"
            , "MEDIUMINT", "INT", "BIGINT", "CHAR", "VARCHAR", "VARBINARY", "BLOB", "TEXT");
    // (n(,m))类型
    public static final List<String> DECIMAL_NEED_IDENT = Arrays.asList("DECIMAL", "NUMERIC");
    // non OR (n,m)
    public static final List<String> DECIMAL_NEED_IDENT_NONSTANDARD = Arrays.asList("FLOAT", "DOUBLE");
    // 包含fsp 小数秒
    public static final List<String> TIME_INCLUDE_FSP = Arrays.asList("TIME", "DATETIME", "TIMESTAMP");

    private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE `%s`";
    private static final String MYSQL_ESCAPE = "`";
    private static final String MYSQL_ESCAPE_AROUND = "`%`";
    private static final String MYSQL_SPACE = " ";

    @Override
    public String convert(BasePipingData pipingData) {
        switch (pipingData.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) pipingData;
                return String.format(CREATE_DATABASE_TEMPLATE, schemaStruct.getSchemaName());
            case TABLE:
                TableStructBasePipingData tableStruct = (TableStructBasePipingData) pipingData;
                // 表结构构建
                return generateMysqlCreateTable(tableStruct);
            case FULL_DATA:
                FullTableDataBasePipingData tableData = (FullTableDataBasePipingData) pipingData;
                return generateMysqlInsert(tableData);
        }
        return null;
    }

    /**
     * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
     *     (create_definition,...)
     *     [table_options]
     *     [partition_options]
     * @param tableStruct
     * @return
     */
    private String generateMysqlCreateTable(TableStructBasePipingData tableStruct) {
        // 构造建表语句，有外键放最后执行
        String sqlFmt = "CREATE TABLE IF NOT EXISTS %s (" +
                "%s) %s ";
        String tbl_name = MYSQL_ESCAPE+tableStruct.getTableName()+MYSQL_ESCAPE;
        StringBuilder column_definition = new StringBuilder();

        for (TableStructBasePipingData.Column column : tableStruct.getColumns()) {
            column_definition.append(fmtColumnName(column.getColumnName())).append(MYSQL_SPACE)
                    .append(combineTypeLength(column)).append(MYSQL_SPACE).append(assembleUnsigned(column))
                    .append(MYSQL_SPACE).append(assembleNull(column)).append(MYSQL_SPACE)
                    .append(assembleAutoIncrement(column)).append(column.getRemarks()).append(",");
        }

        StringBuilder primaryKeyBuilder = new StringBuilder();
        primaryKeyBuilder.append("PRIMARY KEY (");
        for (int i = 0; i < tableStruct.getIndexInfo().getPrimaryKeys().size(); i++) {
            TableStructBasePipingData.Column column = tableStruct.getIndexInfo().getPrimaryKeys().get(i);
            primaryKeyBuilder.append(MYSQL_ESCAPE).append(column.getColumnName()).append(MYSQL_ESCAPE);
            if (i != tableStruct.getIndexInfo().getPrimaryKeys().size() - 1) {
                primaryKeyBuilder.append(",");
            }
        }
        primaryKeyBuilder.append("),");

        StringBuilder uniqueKeyBuilder = new StringBuilder();
        for (String key : tableStruct.getIndexInfo().getUniqueKeys().keySet()) {
            uniqueKeyBuilder.append("UNIQUE KEY `").append(key).append("` (");
            for (int i = 0; i < tableStruct.getIndexInfo().getUniqueKeys().get(key).size(); i++) {
                TableStructBasePipingData.Column column = tableStruct.getIndexInfo().getUniqueKeys().get(key).get(i);
                uniqueKeyBuilder.append("`" + column.getColumnName() + "`");
                if (i != tableStruct.getIndexInfo().getUniqueKeys().get(key).size() - 1) {
                    uniqueKeyBuilder.append(",");
                }
            }
            uniqueKeyBuilder.append("),");
        }

        StringBuilder normalKeyBuilder = new StringBuilder();
        for (String key : tableStruct.getIndexInfo().getNormalKeys().keySet()) {
            uniqueKeyBuilder.append("KEY `").append(key).append("` (");
            for (int i = 0; i < tableStruct.getIndexInfo().getNormalKeys().get(key).size(); i++) {
                TableStructBasePipingData.Column column = tableStruct.getIndexInfo().getNormalKeys().get(key).get(i);
                uniqueKeyBuilder.append("`" + column.getColumnName() + "`");
                if (i != tableStruct.getIndexInfo().getNormalKeys().get(key).size() - 1) {
                    uniqueKeyBuilder.append(",");
                }
            }
            uniqueKeyBuilder.append("),");
        }

        /**
         * MYSQL 默认ENGINE=InnoDB DEFAULT CHARSET=UTF-8 ROW_FORMAT=DYNAMIC COMMENT='' */
        String table_options = "ENGINE=InnoDB DEFAULT CHARSET=UTF8 ROW_FORMAT=DYNAMIC COMMENT='"
                + tableStruct.getTableOption(TableStructBasePipingData.TABLE_OPTION_COMMENT) + "'";
        column_definition.append(primaryKeyBuilder).append(uniqueKeyBuilder).append(normalKeyBuilder);

        return String.format(sqlFmt, tbl_name, column_definition.replace(column_definition.length()-1, column_definition.length(), " "),
                table_options);

    }

    private String combineTypeLength(TableStructBasePipingData.Column column) {
        String typeStr = sqlType2MysqlType(column.getColumnType());
        if (LENGTH_NEED_IDENT.contains(typeStr.toUpperCase()) || TIME_INCLUDE_FSP.contains(typeStr.toUpperCase())) {
            if (column.getColumnSize() == 65535 && column.getColumnType() == Types.LONGVARBINARY) {
                return "BLOB";
            }
            return typeStr + assembleLength(column);
        } else if (DECIMAL_NEED_IDENT.contains(typeStr.toUpperCase())) {
            if (column.getDigits() > 0) {
                return typeStr + "("+column.getColumnSize()+","+column.getDigits()+")";
            }
            return typeStr + assembleLength(column);
        } else if (DECIMAL_NEED_IDENT_NONSTANDARD.contains(typeStr.toUpperCase())) {
            if (column.getColumnSize() > 0 || column.getDigits() > 0) {
                return typeStr + "("+column.getColumnSize()+","+column.getDigits()+")";
            }
            return typeStr + assembleLength(column);
        } else {
            return typeStr;
        }
    }

    private String sqlType2MysqlType(int type) {
        switch (type) {
            case Types.BIT:
            case Types.BOOLEAN:
                return "BIT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
            case Types.REAL:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.NUMERIC:
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.CHAR:
                return "CHAR";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return "TEXT";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
                return "TINYBLOB";
            case Types.VARBINARY:
                return "varbinary";
            case Types.LONGVARBINARY:
                return "varbinary";
            case Types.BLOB:
                return "BLOB";
            case Types.CLOB:
            case Types.NCLOB:
                return "CLOB";
            case Types.OTHER:
                return "BLOB";
            case Types.NCHAR:
                return "CHAR";
            case Types.NVARCHAR:
                return "VARCHAR";

        }
        return "TEXT "; // 默认都走text
    }

    private String assembleLength(TableStructBasePipingData.Column column) {
        if (column.getColumnSize() > 0) {
            return "(" + column.getColumnSize() + ")";
        }
        return "";
    }

    private String assembleUnsigned(TableStructBasePipingData.Column column) {
        if(column.getTypeName().contains("UNSIGNED")) {
            return "UNSIGNED";
        }
        return "";
    }

    private String assembleNull(TableStructBasePipingData.Column column) {
        if (!column.getColumnNullable()) {
            return "NOT NULL";
        }
        return "";
    }

    private String assembleAutoIncrement(TableStructBasePipingData.Column column) {
        if (column.getISAutoIncrement()) {
            return "AUTO_INCREMENT";
        }
        return "";
    }

    private String generateMysqlInsert(FullTableDataBasePipingData tableData) {
        String insertTemplate = "INSERT INTO %s (%s) values (%s) ON DUPLICATE KEY UPDATE ";
        String columnsStr = tableData.getColumns().stream()
                .map(TableStructBasePipingData.Column::getColumnName).collect(Collectors.joining("`,`"));
        String valuesStr = StringUtils.createReplaceStr("?", tableData.getColumns().size());
        StringBuilder sql = new StringBuilder(String.format(insertTemplate, tableData.getTableName(),
                "`" + columnsStr + "`", valuesStr));


        for (int i = 0, size = tableData.getColumns().size(); i < size; i++) {
            String afterFmtColumnName = fmtColumnName(tableData.getColumns().get(i).getColumnName());
            sql.append(afterFmtColumnName)
                    .append("=VALUES(")
                    .append(afterFmtColumnName)
                    .append(")");
            if (i != size - 1) {
                sql.append(",");
            }
        }
        return sql.toString();
    }


    protected String fmtColumnName(String columName) {
        return String.format("`%s`", columName);
    }
}
