package org.min.watergap.outfall.convertor;

import org.min.watergap.common.piping.data.impl.BasePipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.data.impl.TableStructBasePipingData;

public class MysqlStructConvertor implements StructConvertor {

    private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE %s";

    @Override
    public String convert(BasePipingData pipingData) {
        switch (pipingData.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) pipingData;
                return String.format(CREATE_DATABASE_TEMPLATE, schemaStruct.getName());
            case TABLE:
                TableStructBasePipingData tableStruct = (TableStructBasePipingData) pipingData;
                // 表结构构建
                return generateMysqlCreateTable(tableStruct);
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

}
