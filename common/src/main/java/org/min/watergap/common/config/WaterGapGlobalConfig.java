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


    private DataSourceConfig sourceConfig;

    private DataSourceConfig targetConfig;

    private StartMode starterMode;

    private String jsonPath;

    public WaterGapGlobalConfig(String jsonPath) {
        this.jsonPath = jsonPath;
        init();
    }

    public DataSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(DataSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public DataSourceConfig getTargetConfig() {
        return targetConfig;
    }

    public void setTargetConfig(DataSourceConfig targetConfig) {
        this.targetConfig = targetConfig;
    }

    public StartMode getStarterMode() {
        return starterMode;
    }

    public void setStarterMode(StartMode starterMode) {
        this.starterMode = starterMode;
    }

    public void init() {
        JsonParser jsonParser = new JsonParser();
        try {
            JsonObject propJsonObject = jsonParser.parse(new FileReader(jsonPath)).getAsJsonObject();
            sourceConfig.load(propJsonObject.getAsJsonObject(DataSourceConfig.CONFIG_ROOT_SOURCE));
            targetConfig.load(propJsonObject.getAsJsonObject(DataSourceConfig.CONFIG_ROOT_TARGET));
            starterMode = StartMode.valueOf(propJsonObject.get(CONFIG_ROOT_MODE).getAsString());
        } catch (FileNotFoundException fileNotFoundException) {
            throw new WaterGapException("file not found : " + jsonPath, fileNotFoundException);
        }
    }

}
