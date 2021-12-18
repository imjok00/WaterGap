package org.min.watergap.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.min.watergap.common.local.storage.SqliteUtils;

import java.sql.SQLException;

/**
 * TODO:
 *
 * @Create by metaX.h on 2021/12/13 22:53
 */
public class SqliteUtilsTest extends TestCase {

    @Test
    public void testShowTableMetaData() throws SQLException {
        SqliteUtils.executeSQL("select * from FULL_EXTRACTOR_STATUS");
    }

}
