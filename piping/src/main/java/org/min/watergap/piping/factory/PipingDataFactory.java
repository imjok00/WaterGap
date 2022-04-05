package org.min.watergap.piping.factory;

import com.lmax.disruptor.EventFactory;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.rdbms.struct.StructType;

import java.util.Map;

/**
 * 对象创建工厂
 *
 * @Create by metaX.h on 2022/3/30 23:06
 */
public class PipingDataFactory implements EventFactory<PipingData> {

    /**
     * 新建一个占位符对象
     * @return
     */
    @Override
    public PipingData newInstance() {
        return new PipingData() {
            @Override
            public String generateQuerySQL() {
                return null;
            }

            @Override
            public String generateInsertSQL() {
                return null;
            }

            @Override
            public String generateUpdateSQL(Map<String, Object> objectMap) {
                return null;
            }

            @Override
            public StructType getType() {
                return null;
            }

            @Override
            public void onCopy(PipingData data) {

            }
        };
    }
}
