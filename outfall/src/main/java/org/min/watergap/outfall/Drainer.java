package org.min.watergap.outfall;

import org.min.watergap.common.lifecycle.WaterGapLifeCycle;

public interface Drainer extends WaterGapLifeCycle {

    void apply();

}
