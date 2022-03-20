package org.min.watergap.common.position.full;

import org.min.watergap.common.position.Position;

/**
 * 全量迁移时位置记录
 * 不管何种类型的主键都转化成String类型方便拼接查询
 *
 * @Create by metaX.h on 2022/3/20 19:50
 */
public class RdbmsFullPosition implements Position {

    public static final String START_POSITION_FLAG = "0";

    private String position;

    public RdbmsFullPosition(String position) {
        this.position = position;
    }

    @Override
    public String getPosition() {
        return position;
    }

    @Override
    public void setPosition(String position) {
        this.position = position;
    }

}
