package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.intake.full.FullExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 关系形数据库全量数据导出
 *
 * @Create by metaX.h on 2021/11/10 23:16
 */
public abstract class RdbmsFullExtractor extends FullExtractor {
    private static final Logger log = LoggerFactory.getLogger(RdbmsFullExtractor.class);

    protected abstract void extractTableSchema();

    protected abstract void extractTableDatas();

    @Override
    public void extractor() throws WaterGapException {
        // step 1: get all table schema into localstorage
        extractTableSchema();
        // step 2: get all table data into localstorage
        extractTableDatas();
    }

    protected List<TableStruct> getAllTableStruct() throws SQLException {
        final List<TableStruct> result = new ArrayList<>();
        switch (baseConfig.scope) {
            case SOME_TABLE: // 只是迁移部分表
                baseConfig.getScopePairs().forEach(scopePair -> {
                    result.add(new TableStruct(scopePair.getSchemaName(), scopePair.getTableName()));
                });
                break;
            case ALL_DATABASE: // 迁移整个库
                result.addAll(showAllDatabases());
                break;
            case SOME_DATABASE:
                baseConfig.getScopePairs().forEach(scopePair -> {
                    try {
                        result.addAll(showAllTables(scopePair.getSchemaName()));
                    } catch (SQLException e) {
                        log.error("can not get table from schema {}", scopePair.getSchemaName(), e);
                    }
                });
                break;
            default:
                log.warn("scope config is error");
                break;
        }
        return result;
    }

    private List<TableStruct> showAllTables(String catalog) throws SQLException {
        List<TableStruct> tableStructs = new ArrayList<>();
        executeQuery(catalog, dbDialect.SHOW_TABLES(), (resultSet) -> {
            tableStructs.add(new TableStruct(catalog, resultSet.getString(1)));
        });
        return tableStructs;
    }

    private List<TableStruct> showAllDatabases() throws SQLException {
        List<TableStruct> tableStructs = new ArrayList<>();
        executeQuery(dbDialect.SHOW_DATABASES(), (resultSet -> {
            tableStructs.addAll(showAllTables(resultSet.getString(1)));
        }));
        return tableStructs;
    }

    class TableStruct {
        private String schema;
        private String table;
        private String createSql;
        private List<String> primaryKeys;

        public TableStruct(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getCreateSql() {
            return createSql;
        }

        public void setCreateSql(String createSql) {
            this.createSql = createSql;
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }

        public void setPrimaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
        }
    }
}
