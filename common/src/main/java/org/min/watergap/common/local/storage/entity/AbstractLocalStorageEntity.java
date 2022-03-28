package org.min.watergap.common.local.storage.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 存储在本地存储的结果对象抽象
 *
 * @Create by metaX.h on 2021/11/7 16:04
 */
public abstract class AbstractLocalStorageEntity {

    public final static String COMMON_STATUS = "status";

    private int status;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public abstract String getLocalTableName();

    public abstract List<String> getSelectColumns();

    public abstract String getSelectOneCondition();

    public abstract String getInsertValues();

    public abstract AbstractLocalStorageEntity generateObj(ResultSet resultSet) throws SQLException;

    public String generateInsert() {
        String columns = String.join(",", getSelectColumns());
        return String.format("INSERT INTO %s (%s) VALUES (%s)", getLocalTableName(), columns, getInsertValues());
    }

    public String generateQueryOne() {
        String columns = String.join(",", getSelectColumns());
        return String.format("SELECT %s FROM %s WHERE %s", columns, getLocalTableName(), getSelectOneCondition());
    }

    public String generateUpdate(Map<String, Object> values) {
        StringBuilder updateKey = new StringBuilder();
        values.keySet().forEach(key -> {
            updateKey.append(key).append("=");
            Object obj = values.get(key);
            if (obj instanceof String) {
                updateKey.append("'").append(obj).append("'");
            } else if (obj instanceof Integer || obj instanceof Long) {
                updateKey.append(obj);
            }
        });
        String updateSQL = "UPDATE %s SET %s WHERE %s";
        return String.format(updateSQL, getLocalTableName(), updateKey, getSelectOneCondition());
    }

    public static enum LocalStorageStatus {
        INIT(0), COMPLETE(1);

        private int status;

        LocalStorageStatus(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }

        public static LocalStorageStatus valueOf(int value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return COMPLETE;
            }
            return null;
        }

        @Override
        public String toString() {
            return "LocalStorageStatus{" +
                    "status=" + status +
                    '}';
        }
    }
}
