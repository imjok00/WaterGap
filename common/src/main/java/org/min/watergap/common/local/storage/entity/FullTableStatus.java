package org.min.watergap.common.local.storage.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 存储导出的table信息
 *
 * @Create by metaX.h on 2022/3/13 20:53
 */
public class FullTableStatus extends AbstractLocalStorageEntity {

    public static List<String> ALL_COLUMNS
            = Arrays.asList("schemaName", COMMON_STATUS, "tableName", "sourceCreateSql", "offset");

    private static final String TABLE_NAME = "FULL_TABLE_STATUS";

    private String schema;
    private String name;
    private String sourceCreateSql;
    private Long offset;

    public FullTableStatus() {}

    public FullTableStatus(String schema, String name, String sourceCreateSql) {
        this.schema = schema;
        this.name = name;
        this.sourceCreateSql = sourceCreateSql;
    }

    @Override
    public String getLocalTableName() {
        return TABLE_NAME;
    }

    @Override
    public List<String> getSelectColumns() {
        return ALL_COLUMNS;
    }

    @Override
    public String getSelectOneCondition() {
        return "schemaName='" + schema + "' and tableName='" + name + "'";
    }

    @Override
    public String getInsertValues() {
        return String.format("'%s','%s','%s','%s','%s'", schema, name, sourceCreateSql, offset, getOffset());
    }

    @Override
    public AbstractLocalStorageEntity generateObj(ResultSet resultSet) throws SQLException {
        FullTableStatus fullTableStatus = new FullTableStatus();
        fullTableStatus.setSchema(resultSet.getString("schemaName"));
        fullTableStatus.setName(resultSet.getString("tableName"));
        fullTableStatus.setSourceCreateSql(resultSet.getString("sourceCreateSql"));
        fullTableStatus.setStatus(resultSet.getInt("status"));
        fullTableStatus.setOffset(resultSet.getLong("offset"));
        return fullTableStatus;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSourceCreateSql() {
        return sourceCreateSql;
    }

    public void setSourceCreateSql(String sourceCreateSql) {
        this.sourceCreateSql = sourceCreateSql;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    static class Column {
        private String name;
        private int type;

        public Column(String name, int type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }
}
