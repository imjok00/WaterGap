package org.min.watergap.piping.producer;

import com.lmax.disruptor.RingBuffer;
import org.min.watergap.piping.translator.PipingData;

/**
 * pipingdata 生产者封装
 *
 * @Create by metaX.h on 2022/3/31 23:45
 */
public class PipingDataProducer {

    private RingBuffer<PipingData> ringBuffer;

    public PipingDataProducer(RingBuffer<PipingData> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onProduce(PipingData data) {
        long sequence = ringBuffer.next();
        try {
            PipingData emptyData = ringBuffer.get(sequence);
            //emptyData.onCopy(data);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
