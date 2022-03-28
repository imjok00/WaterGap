package org.min.watergap.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.min.watergap.intake.full.rdbms.extractor.MysqlDBAllTypePumper;

import java.sql.SQLException;

/**
 * TODO:
 *
 * @Create by metaX.h on 2021/11/21 22:51
 */
public class MysqlFullExtractorTest extends TestCase {
    //private static final Logger LOG = LogManager.getLogger(MysqlFullExtractorTest.class);

    private static MysqlDBAllTypePumper mysqlFullExtractor;

    @Override
    protected void setUp() throws Exception {
//        LOG.info("setup start...");
//        super.setUp();
//        try {
//            mysqlFullExtractor = new MysqlDBStructPumper();
//            DataSourceConfig baseConfig = new MysqlDataSourceConfig();
//            baseConfig.setIp("localhost");
//            baseConfig.setPort("3306");
//            baseConfig.setUser("root");
//            baseConfig.setPassword("123456");
//            baseConfig.setDatabaseName("test");
//            MysqlDataSource dataSource = new MysqlDataSource(baseConfig);
//            mysqlFullExtractor.setDataSource(dataSource.getDataSource());
//        } catch (Exception e) {
//            // ignore
//            LOG.error("", e);
//        }
    }

    @Test
    public void testShowTableMetaData() throws SQLException {
//        LOG.debug("start show table meta data");
//        List<ColumnStruct> list= mysqlFullExtractor.showTableMetaData("test", "test", "user_t");
//        LOG.debug("test.test column size is " + list.size());
//        list.forEach((columnStruct)->{
//            LOG.info(columnStruct.getColumnName());
//        });
//        LOG.debug("end show table meta data");
    }

}
