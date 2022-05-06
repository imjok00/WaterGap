package org.min.watergap.common.local.storage.orm;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * 增量位点记录
 *
 * @Create by metaX.h on 2022/4/29 23:29
 */
@DatabaseTable(tableName = "INCRE_POSITION")
public class IncrePositionORM {

    /**
     * 全局的position记录
     */
    @DatabaseField(columnName = "position")
    private String position;

    public IncrePositionORM() {}

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }
}
