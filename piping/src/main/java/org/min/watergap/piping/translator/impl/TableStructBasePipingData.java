package org.min.watergap.piping.translator.impl;

import org.min.watergap.common.rdbms.struct.StructType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * 表结构中间对象
 *
 * @Create by metaX.h on 2022/2/26 9:03
 */
public class TableStructBasePipingData extends RdbmsStructBasePipingData {
    public static final String TABLE_OPTION_COMMENT = "COMMENT";

    private List<Column> columns;

    private IndexInfo indexInfo;

    private Map<String, String> tableOptions;

    public TableStructBasePipingData(String schemaName, String tableName) {
        setSchemaName(schemaName);
        setTableName(tableName);
        tableOptions = new HashMap<>();
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void initColumns() {
        this.columns = new ArrayList<>();
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public IndexInfo getIndexInfo() {
        return indexInfo;
    }

    public void setIndexInfo(IndexInfo indexInfo) {
        this.indexInfo = indexInfo;
    }

    public void addColumn(Column column) {
        this.columns.add(column);
    }

    public void addPrimaryKeys(Column column) {
        if (this.indexInfo == null) {
            this.indexInfo = new IndexInfo();
        }
        if (this.indexInfo.getPrimaryKeys() == null) {
            this.indexInfo.setPrimaryKeys(new ArrayList<>());
        }
        this.indexInfo.getPrimaryKeys().add(column);
    }

    public void addTableOption(String key, String val) {
        this.tableOptions.put(key, val);
    }

    public String getTableOption(String key) {
        return this.tableOptions.get(key) == null ? "" : this.tableOptions.get(key);
    }

    public void findFullKey() {
        if (this.indexInfo.getPrimaryKeys() != null) {
            Column maxColumn = this.indexInfo.getPrimaryKeys().get(0);
            for(Column column : this.indexInfo.getPrimaryKeys()) {
                if(column.updateFullKey(maxColumn.getCardinality())) {
                    maxColumn = column;
                }
            }
            maxColumn.setFullKey(true);
        } else if (this.indexInfo.getUniqueKeys() != null) {
            Column maxColumn = null;
            for (String key : this.indexInfo.getUniqueKeys().keySet()) {
                for(Column column : this.indexInfo.getUniqueKeys().get(key)) {
                    if (maxColumn == null) {
                        maxColumn = column;
                    }
                    if(column.updateFullKey(maxColumn.getCardinality())) {
                        maxColumn = column;
                    }
                }
            }
            maxColumn.setFullKey(true);
        }
    }

    public void addUniqueKeys(String keyName, Column column) {
        if (this.indexInfo == null) {
            this.indexInfo = new IndexInfo();
        }
        if (this.indexInfo.getUniqueKeys() == null) {
            this.indexInfo.setUniqueKeys(new HashMap<>());
        }
        List<Column> ukeys = this.indexInfo.getUniqueKeys().get(keyName);
        if (ukeys == null) {
            ukeys = new ArrayList<>();
            this.indexInfo.getUniqueKeys().put(keyName, ukeys);
        }
        ukeys.add(column);

    }

    public void addNormalKeys(String keyName, Column column) {
        if (this.indexInfo == null) {
            this.indexInfo = new IndexInfo();
        }
        if (this.indexInfo.getNormalKeys() == null) {
            this.indexInfo.setNormalKeys(new HashMap<>());
        }
        List<Column> ukeys = this.indexInfo.getNormalKeys().get(keyName);
        if (ukeys == null) {
            ukeys = new ArrayList<>();
            this.indexInfo.getNormalKeys().put(keyName, ukeys);
        }
        ukeys.add(column);

    }

    public void addForeignKes(String keyName, Column column) {
        if (this.indexInfo == null) {
            this.indexInfo = new IndexInfo();
        }
        if (this.indexInfo.getForeignKes() == null) {
            this.indexInfo.setForeignKes(new HashMap<>());
        }
        List<Column> ukeys = this.indexInfo.getForeignKes().get(keyName);
        if (ukeys == null) {
            ukeys = new ArrayList<>();
            this.indexInfo.getForeignKes().put(keyName, ukeys);
        }
        ukeys.add(column);

    }

    @Override
    public StructType getType() {
        return StructType.TABLE;
    }

    public static class Column {
        // 是否作为全量查询的key
        public static final String COLUMN_SIZE = "COLUMN_SIZE";
        public static final String COLUMN_DECIMAL_DIGITS = "DECIMAL_DIGITS";
        public static final String COLUMN_IS_NULLABLE = "IS_NULLABLE";
        public static final String COLUMN_IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
        public static final String COLUMN_REMARKS = "REMARKS";


        // index
        public static final String INDEX_TYPE = "TYPE";
        public static final String INDEX_ORDINAL_POSITION = "ORDINAL_POSITION";
        public static final String INDEX_CARDINALITY = "CARDINALITY";
        public static final String INDEX_NON_UNIQUE = "NON_UNIQUE";
        public static final String INDEX_ASC_OR_DESC = "ASC_OR_DESC";

        public static final List<String> INDEX_METAS = Arrays.asList(INDEX_TYPE, INDEX_ORDINAL_POSITION, INDEX_ASC_OR_DESC, INDEX_CARDINALITY, INDEX_NON_UNIQUE);

        public static final List<String> COLUMN_METAS = Arrays.asList(COLUMN_SIZE, COLUMN_DECIMAL_DIGITS, COLUMN_IS_NULLABLE, COLUMN_IS_AUTOINCREMENT, COLUMN_REMARKS);

        private String columnName;
        private int columnType;
        private String typeName;
        private boolean isFullKey;

        private HashMap<String, Object> columnMeta = new HashMap<>();

        public Column(String columnName, int columnType, String typeName) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.typeName = typeName;
        }

        public Column(String columnName) {
            this.columnName = columnName;
        }

        public void addIndexMetas(ResultSet rs) throws SQLException {
            for (String key : INDEX_METAS) {
                columnMeta.put(key, rs.getObject(key));
            }
        }

        public void addColumnMetas(ResultSet rs) throws SQLException {
            for (String key : COLUMN_METAS) {
                columnMeta.put(key, rs.getObject(key));
            }
        }

        public boolean updateFullKey(long maxCardinality) {
            if((long) columnMeta.getOrDefault(INDEX_CARDINALITY, 0L) > maxCardinality) {
                return true;
            }
            return false;
        }

        public void addMeta(String key, Object val) {
            columnMeta.put(key, val);
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public int getColumnType() {
            return columnType;
        }

        public void setColumnType(int columnType) {
            this.columnType = columnType;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public boolean isFullKey() {
            return isFullKey;
        }

        public void setFullKey(boolean fullKey) {
            isFullKey = fullKey;
        }

        public long getCardinality() {
            return getLongByDefault(INDEX_CARDINALITY, 0);
        }

        public long getColumnSize() {
            return getIntByDefault(COLUMN_SIZE, -1);
        }

        public boolean getColumnNullable() {
            return "YES".equals(getStringByDefault(COLUMN_IS_NULLABLE, "NO"));
        }

        public boolean getISAutoIncrement() {
            return "YES".equals(getStringByDefault(COLUMN_IS_AUTOINCREMENT, "NO"));
        }

        public String getRemarks() {
            return getStringByDefault(COLUMN_REMARKS, "");
        }

        public int getDigits() {
            return getIntByDefault(COLUMN_DECIMAL_DIGITS, 0);
        }

        private int getIntByDefault(String key, int defaultVal) {
            Object val = columnMeta.get(key);
            if (val == null) {
                return defaultVal;
            }
            return (int) val;
        }

        private long getLongByDefault(String key, int defaultVal) {
            Object val = columnMeta.get(key);
            if (val == null) {
                return defaultVal;
            }
            return (long) val;
        }

        private String getStringByDefault(String key, String defVal) {
            Object val = columnMeta.get(key);
            if (val == null) {
                return defVal;
            }
            return (String) val;
        }

        private boolean getBoolByDefault(String key, boolean defVal) {
            Object val = columnMeta.get(key);
            if (val == null) {
                return defVal;
            }
            return (boolean) val;
        }

        public HashMap<String, Object> getColumnMeta() {
            return columnMeta;
        }

        public void setColumnMeta(HashMap<String, Object> columnMeta) {
            this.columnMeta = columnMeta;
        }

        @Override
        public String toString() {
            return "Column{" +
                    "columnName='" + columnName + '\'' +
                    ", columnType=" + columnType +
                    ", typeName='" + typeName + '\'' +
                    '}';
        }
    }


    public static class IndexInfo {

        private List<Column> primaryKeys;

        private Map<String, List<Column>> normalKeys;

        private Map<String, List<Column>> uniqueKeys;

        private Map<String, List<Column>> foreignKes;

        public List<Column> getPrimaryKeys() {
            return primaryKeys;
        }

        public void setPrimaryKeys(List<Column> primaryKeys) {
            this.primaryKeys = primaryKeys;
        }

        public Map<String, List<Column>> getNormalKeys() {
            return normalKeys;
        }

        public void setNormalKeys(Map<String, List<Column>> normalKeys) {
            this.normalKeys = normalKeys;
        }

        public Map<String, List<Column>> getUniqueKeys() {
            return uniqueKeys;
        }

        public void setUniqueKeys(Map<String, List<Column>> uniqueKeys) {
            this.uniqueKeys = uniqueKeys;
        }

        public Map<String, List<Column>> getForeignKes() {
            return foreignKes;
        }

        public void setForeignKes(Map<String, List<Column>> foreignKes) {
            this.foreignKes = foreignKes;
        }
    }

    @Override
    public String toString() {
        return "TableStructBasePipingData{" +
                "columns=" + columns +
                '}';
    }
}
