package org.min.watergap.common.piping;

import org.min.watergap.common.rdbms.struct.StructType;

import java.util.Map;

/**
 * 基础传输对象
 *
 * @Create by metaX.h on 2022/2/23 0:01
 */
public interface PipingData {

    String generateQuerySQL();

    String generateInsertSQL();

    String generateUpdateSQL(Map<String, Object> objectMap);

    StructType getType();

    void onCopy(PipingData data);
}
