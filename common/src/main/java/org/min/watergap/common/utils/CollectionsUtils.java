package org.min.watergap.common.utils;

import java.util.Collection;
import java.util.Map;

/**
 * 集合工具类
 *
 * @Create by metaX.h on 2021/11/8 23:24
 */
public class CollectionsUtils {

    public static boolean isNotEmptyMap(Map object) {
        return object != null && !object.isEmpty();
    }

    public static boolean isNotEmpty(Collection collection) {
        return collection != null && !collection.isEmpty();
    }

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

}
