package org.min.watergap.common.rdbms.inclog;

/**
 * Todo
 *
 * @Create by metaX.h on 2022/6/20 6:34
 */
public class QueryLogIncEvent implements IncEvent {

    private String query;

    public QueryLogIncEvent(String query) {
        this.query = query;
    }

}
