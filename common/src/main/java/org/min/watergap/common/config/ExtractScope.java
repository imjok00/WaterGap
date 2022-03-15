package org.min.watergap.common.config;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 导出数据的范围
 *
 * @Create by metaX.h on 2021/12/12 14:40
 */
public class ExtractScope {

    // 默认值进行整库迁移
    public static ExtractScope DEFAULT_EXTRACT_SCOPE = new ExtractScope(ScopeType.ALL);

    // 迁移的数据的类型
    private ScopeType scopeType;

    // 迁移的数据的范围
    private List<SchemaMap> schemaMaps;

    public ExtractScope(ScopeType scopeType) {
        this.scopeType = scopeType;
    }

    public ScopeType getScopeType() {
        return scopeType;
    }

    public void setScopeType(ScopeType scopeType) {
        this.scopeType = scopeType;
    }

    public List<SchemaMap> getSchemaMaps() {
        return schemaMaps;
    }

    public void setSchemaMaps(List<SchemaMap> schemaMaps) {
        this.schemaMaps = schemaMaps;
    }

    public enum ScopeType {
        ALL(1), PARTIAL_DATABASE(2), PARTIAL_TABLE(3);
        private int type;
        private ScopeType(int type){
            this.type = type;
        }

        public static ScopeType getByType(int type) {
            for (ScopeType st : ScopeType.values()) {
                if (st.type == type) {
                    return st;
                }
            }
            return ALL;
        }
    }

    /**
     * 解析生成的迁移范围
     *
     * {
     *     scopeType :  ALL(1), PARTIAL_DATABASE(2), PARTIAL_TABLE(3);
     *     partialTable : [
     *          {
     *              schemaName : [tableName]
     *          }
     *     ]
     * }
     * @param scopeJson
     * @return
     */
    public static ExtractScope getInstance(JsonElement scopeJson) {
        int type = scopeJson.getAsJsonObject().get(WaterGapGlobalConfig.CONFIG_ROOT_SCOPE).getAsInt();
        ExtractScope extractScope = new ExtractScope(ScopeType.getByType(type));
        if (extractScope.getScopeType() != ScopeType.ALL) {
            JsonArray partialArr = scopeJson.getAsJsonObject()
                    .getAsJsonArray(WaterGapGlobalConfig.CONFIG_ROOT_SCOPE_PARTIAL_TABLE);
            if (partialArr != null) {
                extractScope.setSchemaMaps(SchemaMap.getInstances(partialArr));
            }
        }
        return extractScope;
    }

    public static class SchemaMap {
        private String schemaName;
        private List<String> tableNames;

        public SchemaMap() {}

        public static List<SchemaMap> getInstances(JsonArray partialArr) {
            List<SchemaMap> list = new ArrayList<>(partialArr.size());
            for (JsonElement jsonElement : partialArr) {
                list.add(getInstance(jsonElement.getAsJsonObject()));
            }
            return list;
        }

        public static SchemaMap getInstance(JsonObject partial) {
            Gson gson = new Gson();
            return gson.fromJson(partial, SchemaMap.class);
        }

        public SchemaMap(String schemaName) {
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
