package org.min.watergap.control;


import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.utils.SystemPropUtils;
import org.min.watergap.control.mode.Runner;
import org.min.watergap.control.mode.MigrateModeFactory;

/**
 * 主入口
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public class Launcher {

    private static final String SYS_PROP_JSON_PATH = "JsonPath";

    private static WaterGapGlobalConfig globalConfig;

    private static Runner runner;

    public static void main(String[] args) {
        globalConfig = new WaterGapGlobalConfig(SystemPropUtils.getStr(SYS_PROP_JSON_PATH));
        runner = MigrateModeFactory.chooseRunner(globalConfig);
        runner.init(globalConfig);
        runner.start();
        runner.destroy();
    }



}
