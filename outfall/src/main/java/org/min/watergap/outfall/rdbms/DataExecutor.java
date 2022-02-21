package org.min.watergap.outfall.rdbms;

public interface DataExecutor {

    int execute(String schema, String sql);

}