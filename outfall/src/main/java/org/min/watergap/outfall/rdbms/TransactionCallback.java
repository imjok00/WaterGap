package org.min.watergap.outfall.rdbms;

/**
 * 事务完成时进行调用
 *
 * @Create by metaX.h on 2022/3/10 0:13
 */
public interface TransactionCallback {

    void callbackAck();

}
