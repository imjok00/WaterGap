package org.min.watergap.common.config;

import java.util.List;

/**
 * 基础公用的配置信息
 *
 * @Create by metaX.h on 2021/11/11 23:28
 */
public class BaseConfig {

    // 配置导出的数据级别
    public ExtractScope scope;

    public List<ScopePair> scopePairs;

    public BaseConfig() {

    }

    public ExtractScope getScope() {
        return scope;
    }

    public void setScope(ExtractScope scope) {
        this.scope = scope;
    }

    public List<ScopePair> getScopePairs() {
        return scopePairs;
    }

    public void setScopePairs(List<ScopePair> scopePairs) {
        this.scopePairs = scopePairs;
    }

    public enum ExtractScope {
        ALL_DATABASE(1), SOME_DATABASE(2), SOME_TABLE(3);

        private int type;
        private ExtractScope(int type){
            this.type = type;
        }
    }

    public static class ScopePair {
        private String schemaName;
        private String tableName;

        public ScopePair(String schemaName, String tableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

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
    }

}
