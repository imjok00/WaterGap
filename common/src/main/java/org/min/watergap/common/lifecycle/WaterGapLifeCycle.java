package org.min.watergap.common.lifecycle;

import org.min.watergap.common.context.WaterGapContext;

/**
 * 生命周期方法
 *
 * @Create by metaX.h on 2022/1/27 21:52
 */
public interface WaterGapLifeCycle {

    void init(WaterGapContext waterGapContext);

    void destroy();
}
