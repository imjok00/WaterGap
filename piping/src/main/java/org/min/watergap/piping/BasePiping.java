package org.min.watergap.piping;

import org.min.watergap.intake.full.rdbms.struct.BaseStruct;

/**
 * 基础的传输通道
 *
 * @Create by metaX.h on 2021/11/28 22:14
 */
public interface BasePiping {

    void put(BaseStruct struct);

    BaseStruct poll();

}
