package org.min.watergap.common.context;

/**
 * 系统常量
 *
 * @Create by metaX.h on 2022/3/1 23:24
 */
public class SystemConstant {

    // 系统执行线程池数
    public static final int DEFAULT_EXECUTOR_POOL_NUM = Runtime.getRuntime().availableProcessors() * 2;

    // 默认的limit 大小
    public static final long DEFAULT_SQL_SELECT_LIMIT = 10000;

    // 默认的ringbufferSize 大小
    public static final int DEFAULT_RINGBUFFER_SIZE = 1024;

    // 默认的parserThreadCount 大小
    public static final int DEFAULT_PARSERTHREAD_COUNT = 16;
}
