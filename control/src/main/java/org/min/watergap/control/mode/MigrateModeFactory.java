package org.min.watergap.control.mode;

import org.min.watergap.common.config.WaterGapGlobalConfig;

/**
 * 迁移模式选择
 *
 */
public class MigrateModeFactory {

    private static Runner DEFAULT_STARTER = new FullStarter();

    public static Runner chooseRunner(WaterGapGlobalConfig config) {
        Runner starter;
        switch (config.getStarterMode()) {
            case ONLY_FULL:
                starter = new FullStarter();
                break;
            default:
                starter = DEFAULT_STARTER;
        }
        return starter;
    }

}
