package org.min.watergap.common.config;

import java.util.List;

/**
 * 导出数据的范围
 *
 * @Create by metaX.h on 2021/12/12 14:40
 */
public class ExtractScope {

    // 默认值进行整库迁移
    public static ExtractScope DEFAULT_EXTRACT_SCOPE = new ExtractScope(ScopeType.ALL_DATABASE);

    // 迁移的数据的类型
    private ScopeType scopeType;

    // 迁移的数据的范围
    private List<schemaMap> schemaMaps;

    public ExtractScope(ScopeType scopeType) {
        this.scopeType = scopeType;
    }

    public ScopeType getScopeType() {
        return scopeType;
    }

    public void setScopeType(ScopeType scopeType) {
        this.scopeType = scopeType;
    }

    public List<schemaMap> getSchemaMaps() {
        return schemaMaps;
    }

    public void setSchemaMaps(List<schemaMap> schemaMaps) {
        this.schemaMaps = schemaMaps;
    }

    public enum ScopeType {
        ALL_DATABASE(1), PARTIAL_DATABASE(2), PARTIAL_TABLE(3);
        private int type;
        private ScopeType(int type){
            this.type = type;
        }
    }

    public class schemaMap {
        private String schemaName;
        private List<String> tableNames;

        public schemaMap(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public List<String> getTableNames() {
            return tableNames;
        }

        public void setTableNames(List<String> tableNames) {
            this.tableNames = tableNames;
        }
    }
}
