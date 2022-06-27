package org.min.watergap.common.rdbms.inclog;

import java.util.LinkedList;
import java.util.List;

/**
 * 行数据类型
 *
 * @Create by metaX.h on 2022/6/5 13:18
 */
public class RowUpdateDataIncEvent implements IncEvent {

    private List<BaseIncEvent.ColumnInfo> beforeColumns;

    private List<BaseIncEvent.ColumnInfo> afterColumns;

    public RowUpdateDataIncEvent() {

    }

    static class ColumnInfo {
        public String name;
        public String value;
        public int sqlType;
        public boolean isKey;
    }

    public List<BaseIncEvent.ColumnInfo> getBeforeColumns() {
        return beforeColumns;
    }

    public void setBeforeColumns(List<BaseIncEvent.ColumnInfo> beforeColumns) {
        this.beforeColumns = beforeColumns;
    }

    public List<BaseIncEvent.ColumnInfo> getAfterColumns() {
        return afterColumns;
    }

    public void setAfterColumns(List<BaseIncEvent.ColumnInfo> afterColumns) {
        this.afterColumns = afterColumns;
    }

    public void addAfterColumns(BaseIncEvent.ColumnInfo columnInfo) {
        if (afterColumns == null) {
            afterColumns = new LinkedList<>();
        }
        afterColumns.add(columnInfo);
    }

    public void addBeforeColumns(BaseIncEvent.ColumnInfo columnInfo) {
        if (beforeColumns == null) {
            beforeColumns = new LinkedList<>();
        }
        beforeColumns.add(columnInfo);
    }
}
