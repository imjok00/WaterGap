package org.min.watergap.common.config;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.min.watergap.common.config.mode.StartMode;
import org.min.watergap.common.datasource.config.DataSourceConfig;
import org.min.watergap.common.exception.WaterGapException;

import java.io.FileNotFoundException;
import java.io.FileReader;


public class WaterGapGlobalConfig extends BaseConfig{
    /** 配置项 - 运行模式 */
    public static final String CONFIG_ROOT_MODE = "mode";
    /** 配置项 - 拉取queue数据等待时间 */
    public static final String CONFIG_ROOT_POLLTIMEOUT = "pollTimeout";
    /** 配置项 - 运行的线程数量 */
    public static final String CONFIG_ROOT_THREADNUM = "threadNum";

    private DataSourceConfig sourceConfig;

    private DataSourceConfig targetConfig;

    private StartMode starterMode;

    private String jsonPath;

    public WaterGapGlobalConfig(String jsonPath) {
        if (jsonPath == null) { // 没有设置json配置文件
            jsonPath = this.getClass().getResource("/config.json").getPath();
        }
        this.jsonPath = jsonPath;
        init();
    }

    public DataSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public DataSourceConfig getTargetConfig() {
        return targetConfig;
    }

    public StartMode getStarterMode() {
        return starterMode;
    }

    public void init() {
        JsonParser jsonParser = new JsonParser();
        try {
            JsonObject propJsonObject = jsonParser.parse(new FileReader(jsonPath)).getAsJsonObject();
            sourceConfig = DataSourceConfig.getInstance(propJsonObject.getAsJsonObject(DataSourceConfig.CONFIG_ROOT_SOURCE));
            targetConfig = DataSourceConfig.getInstance(propJsonObject.getAsJsonObject(DataSourceConfig.CONFIG_ROOT_TARGET));
            starterMode = StartMode.valueOf(propJsonObject.get(CONFIG_ROOT_MODE).getAsString());
            setPollTimeout(propJsonObject.get(CONFIG_ROOT_POLLTIMEOUT).getAsLong());
            setPumpThreadNum(propJsonObject.get(CONFIG_ROOT_THREADNUM).getAsInt());
        } catch (FileNotFoundException fileNotFoundException) {
            throw new WaterGapException("file not found : " + jsonPath, fileNotFoundException);
        }
    }

}
