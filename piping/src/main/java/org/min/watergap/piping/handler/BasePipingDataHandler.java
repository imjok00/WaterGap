package org.min.watergap.piping.handler;

import com.lmax.disruptor.WorkHandler;
import org.min.watergap.common.piping.PipingData;

/**
 * 基础handler
 *
 * @Create by metaX.h on 2022/3/30 23:27
 */
public class BasePipingDataHandler implements WorkHandler<PipingData> {


    @Override
    public void onEvent(PipingData pipingData) throws Exception {

    }
}
