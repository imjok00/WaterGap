package org.min.watergap.common.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程工厂
 *
 * @Create by metaX.h on 2022/3/13 11:28
 */
public class CustomThreadFactory implements ThreadFactory {

    private String groupName;

    private AtomicInteger nextId = new AtomicInteger(1);

    public CustomThreadFactory(String prefixName) {
        this.groupName = prefixName + "_thread_";
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(groupName + nextId.incrementAndGet());
    }
}
