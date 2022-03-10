package org.min.watergap.outfall.rdbms;

import java.sql.SQLException;

/**
 * 事务完成时进行调用
 *
 * @Create by metaX.h on 2022/3/10 0:13
 */
public interface PipDataAck {

    void callAck() throws SQLException;

}
