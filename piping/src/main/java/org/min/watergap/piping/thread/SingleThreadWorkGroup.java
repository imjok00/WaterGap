package org.min.watergap.piping.thread;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.WaterGapPiping;
import org.min.watergap.piping.PipingExecutor;

/**
 * 单线程的线程池，作为执行资源提供给外部使用
 *
 * @Create by metaX.h on 2022/4/9 9:12
 */
public class SingleThreadWorkGroup extends AbstractWaterGapLifeCycle {
    private static final Logger LOG = LogManager.getLogger(SingleThreadWorkGroup.class);

    private Thread worker;

    private WaterGapPiping piping;

    public SingleThreadWorkGroup(PipingExecutor executor, WaterGapPiping piping) {
        this.piping = piping;
        worker = new Thread(() -> {
            while (isStart()) {
                PipingData pipingData = null;
                try {
                    pipingData = piping.take();
                } catch (InterruptedException e) {
                    LOG.error("work thread take interrupt, try to stop", e);
                    stop();
                }
                executor.execute(pipingData);
            }
        });
    }

    public void tryAddData(PipingData data) {
        try {
            piping.put(data);
        } catch (InterruptedException e) {
            LOG.error("work thread put interrupt, data is {}", data, e);
        }
    }

    @Override
    public void start() {
        super.start();
        worker.start();
    }

    @Override
    public void init(WaterGapContext waterGapContext) {
    }

    @Override
    public void destroy() {
        stop();
    }
}
