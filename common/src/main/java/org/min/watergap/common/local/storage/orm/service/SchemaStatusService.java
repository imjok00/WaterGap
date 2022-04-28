package org.min.watergap.common.local.storage.orm.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.OrmJdbcHelper;
import org.min.watergap.common.local.storage.orm.SchemaStatusORM;
import org.min.watergap.common.utils.CollectionsUtils;

import java.sql.SQLException;
import java.util.List;

/**
 *
 *
 * @Create by metaX.h on 2022/4/5 20:46
 */
public class SchemaStatusService extends BaseLocalService<SchemaStatusORM> {
    private static final Logger LOG = LogManager.getLogger(SchemaStatusService.class);

    public SchemaStatusService() {
        this.dao = OrmJdbcHelper.getDaoSupport(SchemaStatusORM.class);
    }

    public SchemaStatusORM queryOne(String schema) {
        List<SchemaStatusORM> list = null;
        try {
            list = this.dao.queryForEq("schemaName", schema);
        } catch (SQLException e) {
            LOG.error("query schema status error, schemaName : {}", schema, e);
        }
        if (CollectionsUtils.isNotEmpty(list)) {
            return list.get(0);
        }
        return null;
    }

    public int create(String schema, int status) {
        SchemaStatusORM schemaStatusORM = new SchemaStatusORM();
        schemaStatusORM.setSchemaName(schema);
        schemaStatusORM.setStatus(status);
        try {
            return this.dao.create(schemaStatusORM);
        } catch (SQLException e) {
            LOG.error("create schema status error, schema {}, status {}", schema, status, e);
        }
        return 0;
    }

    public int update(String schema, int status) {
        SchemaStatusORM schemaStatusORM = queryOne(schema);
        if (schemaStatusORM != null) {
            schemaStatusORM.setStatus(status);
            try {
                return this.dao.update(schemaStatusORM);
            } catch (SQLException e) {
                LOG.error("update schema status error, schema {}, status {}", schema, status, e);
            }
        }
        return 0;
    }

}
