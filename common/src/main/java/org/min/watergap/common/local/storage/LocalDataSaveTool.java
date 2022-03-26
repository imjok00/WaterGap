package org.min.watergap.common.local.storage;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.position.Position;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 存储本地执行状态到本地库
 *
 * @Create by metaX.h on 2021/12/14 23:32
 */
public class LocalDataSaveTool {
    private static final Logger LOG = LogManager.getLogger(LocalDataSaveTool.class);

    public static boolean save(PipingData entity) throws SQLException {
        try {
            return SqliteUtils.executeSQL(entity.generateInsertSQL());
        } catch (SQLException e) {
            LOG.error("execute sql : {} error", entity.generateInsertSQL());
            throw e;
        }
    }

    public static AbstractLocalStorageEntity.LocalStorageStatus getLocalDataStatus(PipingData data) throws SQLException {
        Map<String, Object> dataMap = SqliteUtils.executeQueryOne(data.generateQuerySQL());
        if (dataMap == null) {
            return null;
        }
        Object obj = dataMap.get(AbstractLocalStorageEntity.COMMON_STATUS);
        if (obj == null) {
            return null;
        }
        return AbstractLocalStorageEntity.LocalStorageStatus.valueOf((Integer) obj);
    }

    public static boolean updateLocalDataStatus(PipingData data, int status) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("status", status);
        return SqliteUtils.executeSQL(data.generateUpdateSQL(map));
    }

    /**
     * position 暂时做成String的json结构，
     * 主要是为了兼容多主键的情况，但不一定都能兼容，
     * 日后再加一些配套功能来完善
     * @param data
     * @param position
     * @return
     * @throws SQLException
     */
    public static boolean updateLocalDataPosition(PipingData data, Position position) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("offset", position);
        return SqliteUtils.executeSQL(data.generateUpdateSQL(map));
    }
}
