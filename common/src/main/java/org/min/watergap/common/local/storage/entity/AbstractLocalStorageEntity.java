package org.min.watergap.common.local.storage.entity;

import org.min.watergap.common.utils.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 存储在本地存储的结果对象抽象
 *
 * @Create by metaX.h on 2021/11/7 16:04
 */
public abstract class AbstractLocalStorageEntity {

    public abstract String getLocalTableName();

    public abstract List<String> getSelectColumns();

    public abstract AbstractLocalStorageEntity generateObj(ResultSet resultSet) throws SQLException;

    public String generateInsert() {
        String columns = String.join(",", getSelectColumns());
        String paceHolders = StringUtils.createReplaceStr("'%s'", getSelectColumns().size());
        return String.format("INSERT INTO %s (%s) VALUES (%s)", getLocalTableName(), columns, paceHolders);
    }

    public static enum LocalStorageStatus {
        INIT(0), complete(1);

        private int status;

        LocalStorageStatus(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }
}
