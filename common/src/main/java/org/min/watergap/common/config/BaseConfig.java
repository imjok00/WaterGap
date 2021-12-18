package org.min.watergap.common.config;

/**
 * 基础公用的配置信息
 *
 * @Create by metaX.h on 2021/11/11 23:28
 */
public class BaseConfig {

    // 配置导出的数据级别
    private ExtractScope scope;

    public BaseConfig() {

    }

    public ExtractScope getScope() {
        return scope == null ? ExtractScope.DEFAULT_EXTRACT_SCOPE : scope;
    }

    public void setScope(ExtractScope scope) {
        this.scope = scope;
    }

}
