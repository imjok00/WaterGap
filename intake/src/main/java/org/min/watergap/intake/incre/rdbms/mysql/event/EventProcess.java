package org.min.watergap.intake.incre.rdbms.mysql.event;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;

/**
 * 增量事件处理
 *
 * @Create by metaX.h on 2022/5/6 23:05
 */
public class EventProcess implements BinaryLogClient.EventListener {
    @Override
    public void onEvent(Event event) {

    }
}
