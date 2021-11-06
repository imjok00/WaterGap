package org.min.watergap.common.datasource;

import javax.sql.DataSource;
import java.io.IOException;

/**
 * base function for DB connection
 *
 * @Create by metaX.h on 2021/11/6 20:10
 */
public interface DataSourceWrapper {

    /**
     * 获取数据源
     *
     * @return
     */
    DataSource getDataSource();

    /**
     * 归还连接
     */
    void revert() throws IOException;

}
