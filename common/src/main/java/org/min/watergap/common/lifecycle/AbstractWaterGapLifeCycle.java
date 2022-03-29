package org.min.watergap.common.lifecycle;

/**
 * 公共的实现
 *
 * @Create by metaX.h on 2022/3/29 23:40
 */
public abstract class AbstractWaterGapLifeCycle implements WaterGapLifeCycle {

    private volatile boolean running = false;

    @Override
    public boolean isStart() {
        return running;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
    }
}
