package org.min.watergap.common.position.full;

import org.min.watergap.common.position.Position;

/**
 * 全量迁移时位置记录
 * 不管何种类型的主键都转化成String类型方便拼接查询
 *
 * @Create by metaX.h on 2022/3/20 19:50
 */
public class RdbmsFullPosition implements Position {

    public static final String START_POSITION_FLAG = "";

    private String position;

    public RdbmsFullPosition(String postion) {
        this.position = postion;
    }

    @Override
    public String getVal() {
        return this.position;
    }

    @Override
    public void parse(String position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return this.position;
    }

    @Override
    public boolean isFirst() {
        return position == null || "".equals(position);
    }


}
