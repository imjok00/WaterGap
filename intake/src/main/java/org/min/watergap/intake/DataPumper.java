package org.min.watergap.intake;

import org.min.watergap.common.position.Position;

/**
 * 数据导出类
 *
 * @Create by metaX.h on 2022/3/20 18:35
 */
public interface DataPumper extends Pumper {

    void persistPosition(Position position);

}
