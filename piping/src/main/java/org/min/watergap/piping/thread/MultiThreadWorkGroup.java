package org.min.watergap.piping.thread;

import com.lmax.disruptor.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.position.incre.GTIDSet;
import org.min.watergap.common.rdbms.misc.LogEvent;
import org.min.watergap.common.rdbms.misc.binlog.BaseLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.LogContext;
import org.min.watergap.common.rdbms.misc.binlog.LogDecoder;
import org.min.watergap.common.rdbms.misc.binlog.event.DeleteRowsLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.FormatDescriptionLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.UpdateRowsLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.event.WriteRowsLogEvent;
import org.min.watergap.common.thread.CustomThreadFactory;
import org.min.watergap.piping.convertor.mysql.MysqlIncreLogConvert;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.PipingEvent;
import org.min.watergap.piping.translator.WaterGapTransactionPiping;
import org.min.watergap.piping.translator.impl.IncreLogEventPipingData;
import org.min.watergap.piping.translator.impl.TableStructBasePipingData;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class MultiThreadWorkGroup extends AbstractWaterGapLifeCycle {

    private static final Logger LOG = LogManager.getLogger(MultiThreadWorkGroup.class);

    private static final int                  maxFullTimes    = 10;
    private WaterGapTransactionPiping transactionBuffer;

    private int                               parserThreadCount;
    private int                               ringBufferSize;
    private RingBuffer<MessageEvent>          disruptorMsgBuffer;
    private ExecutorService                   parserExecutor;
    private ExecutorService                   stageExecutor;
    private String                            destination;
    private AtomicLong                        eventsPublishBlockingTime;
    private GTIDSet gtidSet;
    private WorkerPool<MessageEvent>          workerPool;
    private BatchEventProcessor<MessageEvent> simpleParserStage;
    private BatchEventProcessor<MessageEvent> sinkStoreStage;
    private LogContext logContext;
    protected boolean                         filterDmlInsert = false;
    protected boolean                         filterDmlUpdate = false;
    protected boolean                         filterDmlDelete = false;

    public MultiThreadWorkGroup(int ringBufferSize, int parserThreadCount,
                                WaterGapTransactionPiping transactionBuffer, String destination,
                                boolean filterDmlInsert, boolean filterDmlUpdate, boolean filterDmlDelete){
        this.ringBufferSize = ringBufferSize;
        this.parserThreadCount = parserThreadCount;
        this.transactionBuffer = transactionBuffer;
        this.destination = destination;
        this.filterDmlInsert = filterDmlInsert;
        this.filterDmlUpdate = filterDmlUpdate;
        this.filterDmlDelete = filterDmlDelete;
    }

    @Override
    public void init(WaterGapContext waterGapContext) {

    }

    @Override
    public void destroy() {

    }

    @Override
    public void start() {
        super.start();
        this.disruptorMsgBuffer = RingBuffer.createSingleProducer(new MessageEventFactory(),
            ringBufferSize,
            new YieldingWaitStrategy());
        int tc = parserThreadCount > 0 ? parserThreadCount : 1;
        this.parserExecutor = Executors.newFixedThreadPool(tc, new CustomThreadFactory("MultiStageCoprocessor-Parser-"
                                                                                      + destination));

        this.stageExecutor = Executors.newFixedThreadPool(2, new CustomThreadFactory("MultiStageCoprocessor-other-"
                                                                                    + destination));
        SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
        ExceptionHandler exceptionHandler = new SimpleFatalExceptionHandler();
        // stage 2
        this.logContext = new LogContext();
        simpleParserStage = new BatchEventProcessor<>(disruptorMsgBuffer,
            sequenceBarrier,
            new SimpleParserStage(logContext));
        simpleParserStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(simpleParserStage.getSequence());

        // stage 3
        SequenceBarrier dmlParserSequenceBarrier = disruptorMsgBuffer.newBarrier(simpleParserStage.getSequence());
        WorkHandler<MessageEvent>[] workHandlers = new DmlParserStage[tc];
        for (int i = 0; i < tc; i++) {
            workHandlers[i] = new DmlParserStage();
        }
        workerPool = new WorkerPool<MessageEvent>(disruptorMsgBuffer,
            dmlParserSequenceBarrier,
            exceptionHandler,
            workHandlers);
        Sequence[] sequence = workerPool.getWorkerSequences();
        disruptorMsgBuffer.addGatingSequences(sequence);

        // stage 4
        SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(sequence);
        sinkStoreStage = new BatchEventProcessor<>(disruptorMsgBuffer, sinkSequenceBarrier, new SinkStoreStage());
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());

        // start work
        stageExecutor.submit(simpleParserStage);
        stageExecutor.submit(sinkStoreStage);
        workerPool.start(parserExecutor);
    }

    public void setBinlogChecksum(int binlogChecksum) {
        if (binlogChecksum != BaseLogEvent.BINLOG_CHECKSUM_ALG_OFF) {
            logContext.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
        }
    }

    @Override
    public void stop() {
        // fix bug #968，对于pool与
        workerPool.halt();
        simpleParserStage.halt();
        sinkStoreStage.halt();
        try {
            parserExecutor.shutdownNow();
            while (!parserExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                if (parserExecutor.isShutdown() || parserExecutor.isTerminated()) {
                    break;
                }

                parserExecutor.shutdownNow();
            }
        } catch (Throwable e) {
            // ignore
        }

        try {
            stageExecutor.shutdownNow();
            while (!stageExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                if (stageExecutor.isShutdown() || stageExecutor.isTerminated()) {
                    break;
                }

                stageExecutor.shutdownNow();
            }
        } catch (Throwable e) {
            // ignore
        }
        super.stop();
    }

    public boolean publish(LogEvent event) {
        if (!isStart()) {
            return false;
        }

        boolean interupted = false;
        long blockingStart = 0L;
        int fullTimes = 0;
        do {
            try {
                long next = disruptorMsgBuffer.tryNext();
                MessageEvent data = disruptorMsgBuffer.get(next);
                data.setOriginEvent(event);

                disruptorMsgBuffer.publish(next);
                if (fullTimes > 0) {
                    eventsPublishBlockingTime.addAndGet(System.nanoTime() - blockingStart);
                }
                break;
            } catch (InsufficientCapacityException e) {
                if (fullTimes == 0) {
                    blockingStart = System.nanoTime();
                }
                // park
                // LockSupport.parkNanos(1L);
                applyWait(++fullTimes);
                interupted = Thread.interrupted();
                if (fullTimes % 1000 == 0) {
                    long nextStart = System.nanoTime();
                    eventsPublishBlockingTime.addAndGet(nextStart - blockingStart);
                    blockingStart = nextStart;
                }
            }
        } while (!interupted && isStart());
        return isStart();
    }

    // 处理无数据的情况，避免空循环挂死
    private void applyWait(int fullTimes) {
        int newFullTimes = fullTimes > maxFullTimes ? maxFullTimes : fullTimes;
        if (fullTimes <= 3) { // 3次以内
            Thread.yield();
        } else { // 超过3次，最多只sleep 1ms
            LockSupport.parkNanos(100 * 1000L * newFullTimes);
        }

    }

    static class MessageEvent {

        private LogEvent originEvent;
        private PipingEvent pipingEvent;
        private boolean          needDmlParse = false;
        private PipingData tableMeta;// 表结构

        public LogEvent getOriginEvent() {
            return originEvent;
        }

        public void setOriginEvent(LogEvent originEvent) {
            this.originEvent = originEvent;
        }

        public PipingEvent getPipingEvent() {
            return pipingEvent;
        }

        public void setPipingEvent(PipingEvent pipingEvent) {
            this.pipingEvent = pipingEvent;
        }

        public boolean isNeedDmlParse() {
            return needDmlParse;
        }

        public void setNeedDmlParse(boolean needDmlParse) {
            this.needDmlParse = needDmlParse;
        }

        public PipingData getTableMeta() {
            return tableMeta;
        }

        public void setTableMeta(PipingData tableMeta) {
            this.tableMeta = tableMeta;
        }
    }

    static class SimpleFatalExceptionHandler implements ExceptionHandler {

        @Override
        public void handleEventException(final Throwable ex, final long sequence, final Object event) {
            // 异常上抛，否则processEvents的逻辑会默认会mark为成功执行，有丢数据风险
            throw new WaterGapException("", ex);
        }

        @Override
        public void handleOnStartException(final Throwable ex) {
        }

        @Override
        public void handleOnShutdownException(final Throwable ex) {
        }
    }


    private class SimpleParserStage implements EventHandler<MessageEvent>, LifecycleAware {

        private LogDecoder decoder;
        private LogContext context;
        private MysqlIncreLogConvert logEventConvert;

        public SimpleParserStage(LogContext context){
            decoder = new LogDecoder(BaseLogEvent.UNKNOWN_EVENT, BaseLogEvent.ENUM_END_EVENT);
            this.context = context;
            if (gtidSet != null) {
                context.setGtidSet(gtidSet);
            }
            logEventConvert = new MysqlIncreLogConvert(Charset.defaultCharset());
        }

        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                BaseLogEvent logEvent = (BaseLogEvent) event.getOriginEvent();

                int eventType = logEvent.getHeader().getType();
                TableStructBasePipingData tableMeta = null;
                boolean needDmlParse = false;
                switch (eventType) {
                    case BaseLogEvent.WRITE_ROWS_EVENT_V1:
                    case BaseLogEvent.WRITE_ROWS_EVENT:
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((WriteRowsLogEvent) logEvent);
                        needDmlParse = true;
                        break;
                    case BaseLogEvent.UPDATE_ROWS_EVENT_V1:
                    case BaseLogEvent.PARTIAL_UPDATE_ROWS_EVENT:
                    case BaseLogEvent.UPDATE_ROWS_EVENT:
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((UpdateRowsLogEvent) logEvent);
                        needDmlParse = true;
                        break;
                    case BaseLogEvent.DELETE_ROWS_EVENT_V1:
                    case BaseLogEvent.DELETE_ROWS_EVENT:
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((DeleteRowsLogEvent) logEvent);
                        needDmlParse = true;
                        break;
                    case BaseLogEvent.ROWS_QUERY_LOG_EVENT:
                        needDmlParse = true;
                        break;
                }
                PipingEvent pipingEvent = logEventConvert.convert(logEvent);

                // 记录一下DML的表结构
                event.setNeedDmlParse(needDmlParse);
                event.setTableMeta(tableMeta);
                event.setPipingEvent(pipingEvent);
            } catch (Throwable e) {
                throw e;
            }
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    private class DmlParserStage implements WorkHandler<MessageEvent>, LifecycleAware {

        @Override
        public void onEvent(MessageEvent event) throws Exception {
            try {
                if (event.isNeedDmlParse()) {
//                    int eventType = event.get().getHeader().getType();
//                    CanalEntry.Entry entry = null;
//                    switch (eventType) {
//                        case LogEvent.ROWS_QUERY_LOG_EVENT:
//                            entry = logEventConvert.parse(event.getEvent(), false);
//                            break;
//                        default:
//                            // 单独解析dml事件
//                            entry = logEventConvert.parseRowsEvent((RowsLogEvent) event.getEvent(), event.getTable());
//                    }
//
//                    event.setEntry(entry);
                }
            } catch (Throwable e) {
                throw e;
            }
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    private class SinkStoreStage implements EventHandler<MessageEvent>, LifecycleAware {

        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                if (event.getPipingEvent() != null) {
                    transactionBuffer.add((IncreLogEventPipingData)event.getPipingEvent());
                }

//                LogEvent logEvent = event.getOriginEvent();
//                if (connection instanceof MysqlConnection && logEvent.getSemival() == 1) {
//                    // semi ack回报
//                    ((MysqlConnection) connection).sendSemiAck(logEvent.getHeader().getLogFileName(),
//                            logEvent.getHeader().getLogPos());
//                }

                // clear for gc
                event.setNeedDmlParse(false);
            } catch (Throwable e) {
                throw e;
            }
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    static class MessageEventFactory implements EventFactory<MessageEvent> {

        public MessageEvent newInstance() {
            return new MessageEvent();
        }
    }

    public void setTransactionBuffer(WaterGapTransactionPiping transactionBuffer) {
        this.transactionBuffer = transactionBuffer;
    }

    public void setEventsPublishBlockingTime(AtomicLong eventsPublishBlockingTime) {
        this.eventsPublishBlockingTime = eventsPublishBlockingTime;
    }

    public void setGtidSet(GTIDSet gtidSet) {
        this.gtidSet = gtidSet;
    }

}
