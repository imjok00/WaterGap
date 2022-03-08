package org.min.watergap.common.piping;

import org.min.watergap.common.rdbms.struct.StructType;

/**
 * 基础传输对象
 *
 * @Create by metaX.h on 2022/2/23 0:01
 */
public interface PipingData {

    String generateQuerySQL();

    String generateInsertSQL();

    StructType getType();
}
