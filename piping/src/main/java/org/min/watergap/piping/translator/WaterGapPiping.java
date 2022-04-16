package org.min.watergap.piping.translator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 导出的结构对象
 *
 * @Create by metaX.h on 2021/11/28 22:18
 */
public class WaterGapPiping implements BasePiping {

    private final BlockingQueue<PipingData> fSink = new LinkedBlockingQueue<>();

    @Override
    public void put(PipingData struct) throws InterruptedException {
        fSink.put(struct);
    }

    @Override
    public PipingData poll(long timeout) throws InterruptedException {
        if (timeout <= 0) {
            return fSink.poll();
        } else {
            return fSink.poll(timeout, TimeUnit.MILLISECONDS);
        }

    }

    @Override
    public PipingData take() throws InterruptedException {
        return fSink.take();
    }

}
