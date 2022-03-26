package org.min.watergap.common.position.full;

import com.google.gson.Gson;
import org.min.watergap.common.position.Position;

import java.util.HashMap;
import java.util.Map;

/**
 * 全量迁移时位置记录
 * 不管何种类型的主键都转化成String类型方便拼接查询
 *
 * @Create by metaX.h on 2022/3/20 19:50
 */
public class RdbmsFullPosition implements Position {

    public static final String START_POSITION_FLAG = "{}";

    private Map<String, String> positionMap;

    public RdbmsFullPosition(String postion) {
        parse(postion);
    }

    public RdbmsFullPosition(Map<String, String> keyValMap) {
        positionMap = keyValMap;
    }

    @Override
    public String getVal(String key) {
        if (positionMap == null) {
            return null;
        }
        return positionMap.get(key);
    }

    @Override
    public void parse(String position) {
        Gson gson = new Gson();
        positionMap = gson.fromJson(position, HashMap.class);
    }

    public boolean isStartPosition() {
        return positionMap == null || positionMap.isEmpty();
    }
}
