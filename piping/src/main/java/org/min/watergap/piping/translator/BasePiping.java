package org.min.watergap.piping.translator;

/**
 * 基础的传输通道
 *
 * @Create by metaX.h on 2021/11/28 22:14
 */
public interface BasePiping {

    void put(PipingData data) throws InterruptedException;

    PipingData poll(long timeout) throws InterruptedException;

    PipingData take() throws InterruptedException;

}
