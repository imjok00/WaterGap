package org.min.watergap.piping.thread;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.local.storage.orm.service.FullTableDataPositionService;
import org.min.watergap.common.local.storage.orm.service.FullTableStatusService;
import org.min.watergap.common.local.storage.orm.service.MigrateStageService;
import org.min.watergap.common.local.storage.orm.service.SchemaStatusService;
import org.min.watergap.common.utils.ThreadLocalUtils;
import org.min.watergap.piping.PipingExecutor;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.WaterGapPiping;

/**
 * 单线程的线程池，作为执行资源提供给外部使用
 *
 * @Create by metaX.h on 2022/4/9 9:12
 */
public class SingleThreadWorkGroup extends AbstractWaterGapLifeCycle {
    private static final Logger LOG = LogManager.getLogger(SingleThreadWorkGroup.class);

    private Thread worker;

    private WaterGapPiping piping;

    // 存储在本地的基础对象
    protected FullTableStatusService fullTableStatusService;
    protected FullTableDataPositionService fullTableDataPositionService;
    protected SchemaStatusService schemaStatusService;
    protected MigrateStageService migrateStageService;

    public SingleThreadWorkGroup(PipingExecutor executor, WaterGapPiping outPiping) {
        this.piping = outPiping;
        worker = new Thread(() -> {
            setLocalService();
            while (isStart()) {
                PipingData pipingData = null;
                try {
                    pipingData = piping.take();
                    executor.execute(pipingData);
                } catch (InterruptedException interruptedException) {
                    LOG.warn("interrupt piping take");
                    stop();
                } catch (Exception e) {
                    LOG.error("work thread take interrupt, try to stop", e);
                    destroy();
                }
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
        fullTableStatusService = new FullTableStatusService();
        schemaStatusService = new SchemaStatusService();
        migrateStageService = new MigrateStageService();
        fullTableDataPositionService = new FullTableDataPositionService();
    }

    private void setLocalService() {
        ThreadLocalUtils.set(FullTableStatusService.class.getName(), fullTableStatusService);
        ThreadLocalUtils.set(FullTableDataPositionService.class.getName(), fullTableDataPositionService);
        ThreadLocalUtils.set(SchemaStatusService.class.getName(), schemaStatusService);
        ThreadLocalUtils.set(MigrateStageService.class.getName(), migrateStageService);
    }

    @Override
    public void destroy() {
        stop();
        worker.interrupt();
    }
}
