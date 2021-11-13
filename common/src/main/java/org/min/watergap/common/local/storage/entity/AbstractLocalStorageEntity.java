package org.min.watergap.common.local.storage.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 存储在本地存储的结果对象抽象
 *
 * @Create by metaX.h on 2021/11/7 16:04
 */
public abstract class AbstractLocalStorageEntity {

    public abstract String getTableName();

    public abstract List<String> getSelectColumns();

    public abstract AbstractLocalStorageEntity generateObj(ResultSet resultSet) throws SQLException;
}
