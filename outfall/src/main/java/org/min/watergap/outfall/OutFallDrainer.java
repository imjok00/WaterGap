package org.min.watergap.outfall;

import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.StructPiping;
import org.min.watergap.common.piping.data.impl.BasePipingData;
import org.min.watergap.outfall.rdbms.DataExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OutFallDrainer implements Drainer {
    private static final Logger LOG = LoggerFactory.getLogger(OutFallDrainer.class);

    protected DataExecutor dataExecutor;

    protected StructPiping structPiping;

    protected Long pollTimeout;

    protected DatabaseType targetDBType;

    protected abstract void doExecute(BasePipingData dataStruct);

    @Override
    public void destroy() {
        dataExecutor.destroy();
    }

    @Override
    public void isStart() {

    }

    @Override
    public void apply() {
        try {
            PipingData pipingData = structPiping.poll(pollTimeout);
            doExecute((BasePipingData) pipingData);
        } catch (InterruptedException e) {
            LOG.error("poll event from fsink fail", e);
        }
    }
}