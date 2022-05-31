package org.min.watergap.piping.translator.impl;

import org.min.watergap.common.position.Position;
import org.min.watergap.common.rdbms.struct.StructType;

/**
 * 增量日志位点记录对象
 *
 * @Create by metaX.h on 2022/5/1 23:55
 */
public class IncreLogPositionPipingData extends BasePipingData {

    private Position position;

    public IncreLogPositionPipingData(Position position) {
        this.position = position;
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    @Override
    public StructType getType() {
        return StructType.INCRE_POSITION;
    }
}
