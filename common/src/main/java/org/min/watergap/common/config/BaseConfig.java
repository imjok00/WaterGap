package org.min.watergap.common.config;

/**
 * 基础公用的配置信息
 *
 * @Create by metaX.h on 2021/11/11 23:28
 */
public abstract class BaseConfig {

    private static final long DEFAULT_POLL_TIMEOUT = 500L;

    // 配置导出的数据级别
    private ExtractScope scope;

    private Long pollTimeout;

    public BaseConfig() {

    }

    public ExtractScope getScope() {
        return scope == null ? ExtractScope.DEFAULT_EXTRACT_SCOPE : scope;
    }

    public void setScope(ExtractScope scope) {
        this.scope = scope;
    }

    public Long getPollTimeout() {
        return pollTimeout == null ? DEFAULT_POLL_TIMEOUT : pollTimeout;
    }
}
