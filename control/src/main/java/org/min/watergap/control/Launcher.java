package org.min.watergap.control;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.utils.SystemPropUtils;
import org.min.watergap.control.mode.MigrateModeFactory;
import org.min.watergap.control.mode.Runner;

/**
 * 主入口
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public class Launcher {
    private static final Logger LOG = LogManager.getLogger(Launcher.class);

    private static final String SYS_PROP_JSON_PATH = "JsonPath";

    public static void main(String[] args) {
        LOG.info("## Launcher start ...");
        WaterGapGlobalConfig globalConfig = new WaterGapGlobalConfig(SystemPropUtils.getStr(SYS_PROP_JSON_PATH));
        WaterGapContext waterGapContext = new WaterGapContext(globalConfig);
        LOG.info("## run mode is {}", waterGapContext.getStartMode());
        Runner runner = MigrateModeFactory.chooseRunner(waterGapContext.getStartMode());
        runner.init(waterGapContext);
        try {
            runner.start();
            runner.waitForShutdown();
        } catch (Exception e) {
            LOG.error("Stop runner!!!", e);
        } finally {
            runner.destroy();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(runner::destroy));

    }

}
