package org.min.watergap.piping.translator.impl;

/**
 * 数据结构中间对象
 *
 * @Create by metaX.h on 2022/2/23 23:26
 */
public abstract class RdbmsStructBasePipingData extends BasePipingData {

    private String schemaName;

    private String tableName;

    private String sourceCreateSql;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSourceCreateSql() {
        return sourceCreateSql;
    }

    public void setSourceCreateSql(String sourceCreateSql) {
        this.sourceCreateSql = sourceCreateSql;
    }

    @Override
    public String toString() {
        return "RdbmsStructBasePipingData{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", sourceCreateSql='" + sourceCreateSql + '\'' +
                '}';
    }
}
