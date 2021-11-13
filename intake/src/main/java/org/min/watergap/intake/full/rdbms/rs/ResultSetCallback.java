package org.min.watergap.intake.full.rdbms.rs;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQL 查询结果处理类
 *
 * @Create by metaX.h on 2021/11/13 22:48
 */
public interface ResultSetCallback {

    void callBack(ResultSet resultSet) throws SQLException;

}
