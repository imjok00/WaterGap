package org.min.watergap.piping.handler;

import com.lmax.disruptor.WorkHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.piping.translator.PipingData;

/**
 * 基础handler
 *
 * @Create by metaX.h on 2022/3/30 23:27
 */
public class BasePipingDataHandler implements WorkHandler<PipingData> {
    private static final Logger LOG = LogManager.getLogger(BasePipingDataHandler.class);

    @Override
    public void onEvent(PipingData pipingData) throws Exception {
        LOG.info("{} Test Consumer event {}", this.toString(), pipingData);
    }
}
