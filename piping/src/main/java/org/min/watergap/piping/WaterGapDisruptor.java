package org.min.watergap.piping;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.piping.factory.PipingDataFactory;
import org.min.watergap.piping.handler.BasePipingDataHandler;
import org.min.watergap.piping.producer.PipingDataProducer;

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
            DaemonThreadFactory.INSTANCE, ProducerType.SINGLE, DEFAULT_WAITSTRATEGY);

    public final PipingDataProducer PIPINGDATA_PRODUCER;

    public WaterGapDisruptor(BasePipingDataHandler... pipingDataHandlers) {
        DISRUPTOR.handleEventsWithWorkerPool(pipingDataHandlers);
        DISRUPTOR.start();
        PIPINGDATA_PRODUCER = new PipingDataProducer(DISRUPTOR.getRingBuffer());
    }

    public void addPipingData(PipingData data) {
        PIPINGDATA_PRODUCER.onProduce(data);
    }
}
