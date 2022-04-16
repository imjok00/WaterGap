package org.min.watergap.common.utils;

import org.min.watergap.common.local.storage.orm.service.FullTableDataPositionService;
import org.min.watergap.common.local.storage.orm.service.FullTableStatusService;
import org.min.watergap.common.local.storage.orm.service.MigrateStageService;
import org.min.watergap.common.local.storage.orm.service.SchemaStatusService;

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

    public static FullTableStatusService getFullTableStatusService() {
        return (FullTableStatusService) ThreadLocalUtils.get(FullTableStatusService.class.getName());
    }

    public static SchemaStatusService getSchemaStatusService() {
        return (SchemaStatusService) ThreadLocalUtils.get(SchemaStatusService.class.getName());
    }

    public static MigrateStageService getMigrateStageService() {
        return (MigrateStageService) ThreadLocalUtils.get(MigrateStageService.class.getName());
    }
}
