package org.min.watergap.common.rdbms.inclog;

/**
 * Todo
 *
 * @Create by metaX.h on 2022/6/6 0:10
 */
public class BaseIncEvent implements IncEvent {


    public static class ColumnInfo {
        public String name;
        public String value;
        public int sqlType;
        public boolean isKey;
        public int index;
        public boolean isNull;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getSqlType() {
            return sqlType;
        }

        public void setSqlType(int sqlType) {
            this.sqlType = sqlType;
        }

        public boolean isKey() {
            return isKey;
        }

        public void setKey(boolean key) {
            isKey = key;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public boolean isNull() {
            return isNull;
        }

        public void setNull(boolean aNull) {
            isNull = aNull;
        }
    }
}
