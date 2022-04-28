package org.min.watergap.common.local.storage.orm.service;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.stmt.QueryBuilder;

import java.sql.SQLException;

/**
 * Todo
 *
 * @Create by metaX.h on 2022/4/25 22:57
 */
public class BaseLocalService<T> {

    protected Dao<T, Long> dao;

    public boolean isAllComplete() throws SQLException {
        QueryBuilder queryBuilder = dao.queryBuilder();
        queryBuilder.where().ne("status",  MigrateStageService.LocalStorageStatus.COMPLETE.getStatus());
        return 0 == queryBuilder.query().size();
    }

}
