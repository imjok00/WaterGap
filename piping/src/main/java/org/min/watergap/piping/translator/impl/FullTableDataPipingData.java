package org.min.watergap.piping.translator.impl;

import org.min.watergap.common.position.Position;
import org.min.watergap.common.position.full.RdbmsFullPosition;
import org.min.watergap.common.rdbms.struct.StructType;
import org.min.watergap.common.utils.CollectionsUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据传输对象
 *
 * @Create by metaX.h on 2022/3/20 20:53
 */
public class FullTableDataPipingData extends TableStructBasePipingData {

    private Position position;

    private ColumnValContain contain;

    private boolean needInit;

    public FullTableDataPipingData(TableStructBasePipingData tableStructBasePipingData) {
        super(tableStructBasePipingData.getSchemaName(), tableStructBasePipingData.getTableName());
        setSourceCreateSql(tableStructBasePipingData.getSourceCreateSql());
        setColumns(tableStructBasePipingData.getColumns());
        setIdentical(tableStructBasePipingData.isIdentical());
        setIndexInfo(tableStructBasePipingData.getIndexInfo());
        position = new RdbmsFullPosition(RdbmsFullPosition.START_POSITION_FLAG);
        
    }

    public FullTableDataPipingData(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public void setNeedInit(boolean needInit) {
        this.needInit = needInit;
    }

    public boolean isNeedInit() {
        return needInit;
    }

    public void setContain(ColumnValContain contain) {
        this.contain = contain;

        if (getIndexInfo() == null) {
            return;
        }

        List<Column> primaryKeys = getIndexInfo().getPrimaryKeys();
        if (CollectionsUtils.isEmpty(primaryKeys)) {
            return;
        }
        if (CollectionsUtils.isEmpty(contain.getValMapList())) {
            return;
        }
        Map<String, Object> oneColumnMap = contain.getValMapList().get(contain.getValMapList().size() - 1);
        primaryKeys.forEach(column -> {
            if (column.isFullKey()) {
                Position position = new RdbmsFullPosition(String.valueOf(oneColumnMap.get(column.getColumnName())));
                setPosition(position);
            }
        });

    }

    public ColumnValContain getContain() {
        return contain;
    }

    public static class ColumnValContain {
        List<Map<String, Object>> valMapList;
        List<Column> columns;

        public ColumnValContain(List<Column> columns) {
            this.columns = columns;
            this.valMapList = new ArrayList<>();
        }

        public void addVal(Map<String, Object> map) {
            valMapList.add(map);
        }

        public List<Map<String, Object>> getValMapList() {
            return valMapList;
        }

        public void setValMapList(List<Map<String, Object>> valMapList) {
            this.valMapList = valMapList;
        }

        public List<Column> getColumns() {
            return columns;
        }

        public void setColumns(List<Column> columns) {
            this.columns = columns;
        }

        public boolean isEmpty() {
            return CollectionsUtils.isEmpty(valMapList);
        }
    }

    @Override
    public StructType getType() {
        return StructType.FULL_DATA;
    }

    @Override
    public String toString() {
        return "{" +
                "schema=" + getSchemaName() +
                "; table=" + getTableName() +
                "; position=" + position +
                '}';
    }
}
