package org.min.watergap.intake.full.rdbms.mysql;

import org.min.watergap.intake.dialect.MysqlDialect;
import org.min.watergap.intake.full.rdbms.RdbmsFullExtractor;

import java.sql.Connection;
import java.sql.Statement;

/**
 * mysql 全量数据导出类
 *
 * @Create by metaX.h on 2021/11/4 23:30
 */
public class MysqlFullExtractor extends RdbmsFullExtractor {


    @Override
    protected void extractTableSchema() {


        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ) {
            statement.executeQuery(MysqlDialect.GET_ALL_SCHEMA);

        } catch (Exception e) {

        }
    }

    @Override
    protected void extractTableDatas() {

    }
}
