package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.position.full.RdbmsFullPosition;

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
}
