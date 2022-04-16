package org.min.watergap.piping;

import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.min.watergap.piping.factory.PipingDataFactory;
import org.min.watergap.piping.producer.PipingDataProducer;
import org.min.watergap.piping.translator.PipingData;

import java.util.HashMap;
import java.util.Map;

/**
 * 调度操作
 *
 * @Create by metaX.h on 2022/3/30 22:51
 */
public class WaterGapDisruptor {

    private static final PipingDataFactory PIPINGDATA_FACTORY = new PipingDataFactory();

    private static YieldingWaitStrategy DEFAULT_WAITSTRATEGY = new YieldingWaitStrategy();

    private static final int DEFAULT_BUFFERSIZE = 1024 << 2;

    private static final Disruptor<PipingData> DISRUPTOR = new Disruptor<>(PIPINGDATA_FACTORY, DEFAULT_BUFFERSIZE,
            DaemonThreadFactory.INSTANCE, ProducerType.MULTI, DEFAULT_WAITSTRATEGY);

    public final PipingDataProducer PIPINGDATA_PRODUCER;

    private final Map<Thread, PipingDataProducer> producerMap;

    public WaterGapDisruptor(WorkHandler<PipingData>... pipingDataHandlers) {
        DISRUPTOR.handleEventsWithWorkerPool(pipingDataHandlers);
        DISRUPTOR.start();
        PIPINGDATA_PRODUCER = new PipingDataProducer(DISRUPTOR.getRingBuffer());
        producerMap = new HashMap<>();
    }

    public PipingDataProducer getProducer(Thread thread) {
        PipingDataProducer producer = producerMap.get(thread);
        if (producer == null) {
            producer = new PipingDataProducer(DISRUPTOR.getRingBuffer());
            producerMap.put(thread, producer);
        }
        return producer;
    }
}
