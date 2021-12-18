package org.min.watergap.piping.full;

import org.min.watergap.intake.full.rdbms.struct.BaseStruct;
import org.min.watergap.intake.full.rdbms.struct.SchemaStruct;
import org.min.watergap.piping.BasePiping;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 全量阶段数据传输
 *
 * @Create by metaX.h on 2021/11/28 22:18
 */
public class FullPiping implements BasePiping {

    private ConcurrentHashMap<String, LinkedBlockingQueue<BaseStruct>> fSink = new ConcurrentHashMap<>();

    @Override
    public void put(BaseStruct struct) {
        switch (struct.getType()) {
            case table:

                break;
            case schema:
                SchemaStruct schemaStruct = (SchemaStruct) struct;
                fSink.get(schemaStruct.getSchemaName());
                break;

        }
    }

    @Override
    public BaseStruct poll() {
        return null;
    }
}
