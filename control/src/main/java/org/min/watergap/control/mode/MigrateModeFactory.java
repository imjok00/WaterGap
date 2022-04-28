package org.min.watergap.control.mode;

import org.min.watergap.common.config.mode.StartMode;

/**
 * 迁移模式选择
 *
 */
public class MigrateModeFactory {

    private static Runner DEFAULT_STARTER = new FullStarter();

    public static Runner chooseRunner(StartMode startMode) {
        Runner starter;
        switch (startMode) {
            case ONLY_FULL:
                starter = new FullStarter();
                break;
            case TRANSFER:
                starter = new FullAndIncreStarter();
                break;
            default:
                starter = DEFAULT_STARTER;
        }
        return starter;
    }

}
