package org.min.watergap.outfall.convertor;

import org.min.watergap.common.config.DatabaseType;

import java.util.HashMap;

public class ConvertorChooser {

    private static HashMap<DatabaseType, StructConvertor> convertorHashMap = new HashMap<>();

    static {
        convertorHashMap.put(DatabaseType.MySQL, new MysqlStructConvertor());
    }

    public static StructConvertor chooseConvertor(DatabaseType databaseType) {
        return convertorHashMap.get(databaseType);
    }

}
