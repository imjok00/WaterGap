package org.min.watergap.outfall.convertor;

import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.BasePipingData;
import org.min.watergap.common.piping.struct.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.common.utils.StringUtils;

import java.util.stream.Collectors;

public class MysqlStructConvertor implements StructConvertor {

    private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE %s";

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
        if (tableStruct.isIdentical()) {
            return tableStruct.getSourceCreateSql();
        } else {
            // 异构表生成策略
            String sqlFmt = "CREATE TABLE IF NOT EXISTS %s (" +
                    "%s) %s ";
            String tbl_name = tableStruct.getTableName();
            StringBuilder create_definition = new StringBuilder();

            for (TableStructBasePipingData.Column column : tableStruct.getColumns()) {

            }

            String table_options = null;
            String partition_options = null;

            return String.format(sqlFmt, tbl_name, create_definition, table_options, partition_options);
        }
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
