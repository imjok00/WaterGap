package org.min.watergap.outfall;

import com.lmax.disruptor.WorkHandler;
import org.min.watergap.common.piping.PipingData;

public interface Drainer extends WorkHandler<PipingData> {

}
