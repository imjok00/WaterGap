package org.min.watergap.piping.convertor.pip;

import org.min.watergap.intake.full.rdbms.struct.BaseStruct;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 导出的结构对象
 *
 * @Create by metaX.h on 2021/11/28 22:18
 */
public class StructPiping implements BasePiping {

    private final BlockingQueue<BaseStruct> fSink = new LinkedBlockingQueue<>();

    @Override
    public void put(BaseStruct struct) throws InterruptedException {

        fSink.put(struct);
    }

    @Override
    public BaseStruct poll(long timeout) throws InterruptedException {
        if (timeout <= 0) {
            return fSink.poll();
        } else {
            return fSink.poll(timeout, TimeUnit.MILLISECONDS);
        }

    }
}
