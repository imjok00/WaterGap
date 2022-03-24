package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.position.full.RdbmsFullPosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据传输对象
 *
 * @Create by metaX.h on 2022/3/20 20:53
 */
public class TableDataBasePipingData extends TableStructBasePipingData {

    private Position position;

    public TableDataBasePipingData(TableStructBasePipingData tableStructBasePipingData) {
        super(tableStructBasePipingData.getSchemaName(), tableStructBasePipingData.getTableName());
        setSourceCreateSql(tableStructBasePipingData.getSourceCreateSql());
        setColumns(tableStructBasePipingData.getColumns());
        setIdentical(tableStructBasePipingData.isIdentical());
        setIndexInfo(tableStructBasePipingData.getIndexInfo());
        position = new RdbmsFullPosition(RdbmsFullPosition.START_POSITION_FLAG);
        
    }

    public TableDataBasePipingData(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
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
    }
}
