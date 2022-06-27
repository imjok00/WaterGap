package org.min.watergap.common.rdbms.inclog;

import org.min.watergap.common.sql.parse.DdlResult;

import java.util.List;

/**
 * Query事件
 *
 * @Create by metaX.h on 2022/6/16 23:17
 */
public class DDLIncEvent implements IncEvent {

    private String schema;

    private List<DdlResult> ddlResults;

    public DDLIncEvent(String schema, List<DdlResult> list) {
        this.schema = schema;
        this.ddlResults = list;
    }
}
