package org.min.watergap.control.mode;

import org.min.watergap.common.lifecycle.WaterGapLifeCycle;

/**
 * 任务启动入口
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public interface Runner extends WaterGapLifeCycle {

    /**
     * 启动方法
     */
    void start();

    /**
     * 等待结束
     * @throws InterruptedException
     */
    void waitForShutdown() throws InterruptedException;

}
