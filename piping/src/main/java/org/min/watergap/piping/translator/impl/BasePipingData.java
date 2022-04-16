package org.min.watergap.piping.translator.impl;

import org.min.watergap.piping.translator.PipingData;

/**
 * 传输对象
 *
 * @Create by metaX.h on 2022/2/23 23:24
 */
public abstract class BasePipingData implements PipingData {

    /**
     * 是否源数据库和目标数据库时相同类型的数据库
     */
    private boolean isIdentical;

    public boolean isIdentical() {
        return isIdentical;
    }

    public void setIdentical(boolean identical) {
        isIdentical = identical;
    }
}
