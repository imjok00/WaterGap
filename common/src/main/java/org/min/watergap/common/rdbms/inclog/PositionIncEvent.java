package org.min.watergap.common.rdbms.inclog;

import org.min.watergap.common.position.Position;

import java.util.HashMap;
import java.util.Map;

/**
 * Todo
 *
 * @Create by metaX.h on 2022/6/26 0:09
 */
public class PositionIncEvent implements IncEvent{

    private Position position;

    private Map<String, String> properties;

    public PositionIncEvent(Position position) {
        this.position = position;
        properties = new HashMap<>();
    }

    public void addProperties(String key, String val) {
        properties.put(key, val);
    }

}
