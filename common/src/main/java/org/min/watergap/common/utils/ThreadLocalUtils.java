package org.min.watergap.common.utils;

import org.min.watergap.common.local.storage.orm.FullTableDataPositionORM;
import org.min.watergap.common.local.storage.orm.SchemaStatusORM;
import org.min.watergap.common.local.storage.orm.service.FullTableDataPositionService;
import org.min.watergap.common.local.storage.orm.service.FullTableStatusService;
import org.min.watergap.common.local.storage.orm.service.MigrateStageService;
import org.min.watergap.common.local.storage.orm.service.SchemaStatusService;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 存放线程本地变量
 *
 * @Create by metaX.h on 2022/4/16 14:42
 */
public class ThreadLocalUtils {

    private static final ThreadLocal<Map<String, Object>> THREAD_LOCAL = ThreadLocal.withInitial(HashMap::new);

    public static Object get(String key) {
        return THREAD_LOCAL.get().get(key);
    }

    public static void set(String key, Object object) {
        THREAD_LOCAL.get().put(key, object);
    }

    public static FullTableDataPositionService getFullTableDataPositionService() {
        return (FullTableDataPositionService) ThreadLocalUtils.get(FullTableDataPositionService.class.getName());
    }

    public static void tryCreateDataPosition(FullTableDataPositionORM obj) throws SQLException {
        ThreadLocalUtils.getFullTableStatusService().update(obj.getSchemaName(),
                obj.getTableName(), MigrateStageService.LocalStorageStatus.COMPLETE.getStatus());
        if (null == ThreadLocalUtils.getFullTableDataPositionService().queryLastPosition(obj.getSchemaName(), obj.getTableName())) {
            ThreadLocalUtils.getFullTableDataPositionService().create(
                    new FullTableDataPositionORM(obj.getSchemaName(), obj.getTableName(), ""));
        }
    }



    public static FullTableStatusService getFullTableStatusService() {
        return (FullTableStatusService) ThreadLocalUtils.get(FullTableStatusService.class.getName());
    }

    public static SchemaStatusService getSchemaStatusService() {
        return (SchemaStatusService) ThreadLocalUtils.get(SchemaStatusService.class.getName());
    }

    public static void tryCreateSchemaStatus(String schema) {
        SchemaStatusORM schemaStatusORM = ThreadLocalUtils.getSchemaStatusService().queryOne(schema);
        if (schemaStatusORM == null) { // 还没有初始化
            ThreadLocalUtils.getSchemaStatusService().create(schema, MigrateStageService.LocalStorageStatus.INIT.getStatus());
        }
    }

    public static MigrateStageService getMigrateStageService() {
        return (MigrateStageService) ThreadLocalUtils.get(MigrateStageService.class.getName());
    }
}
