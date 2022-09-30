package org.min.watergap.common.config;

import com.google.gson.JsonElement;
import org.min.watergap.common.context.SystemConstant;

/**
 * 基础公用的配置信息
 *
 * @Create by metaX.h on 2021/11/11 23:28
 */
public abstract class BaseConfig {

    private static final long DEFAULT_POLL_TIMEOUT = 500L;

    // 配置导出的数据级别
    private ExtractScope scope;

    private Integer executorWorkNum;

    private Long sqlSelectLimit;

    private Integer ringbufferSize;

    private Integer parserThreadCount;

    public BaseConfig() {

    }

    public ExtractScope getScope() {
        return scope;
    }

    public void setScope(JsonElement scopeJson) {
        this.scope = scopeJson == null ? ExtractScope.DEFAULT_EXTRACT_SCOPE : ExtractScope.getInstance(scopeJson);
    }

    public Integer getExecutorWorkNum() {
        return executorWorkNum;
    }

    public void setExecutorWorkNum(JsonElement executorWorkNumJson) {
        this.executorWorkNum = executorWorkNumJson == null ? SystemConstant.DEFAULT_EXECUTOR_POOL_NUM : executorWorkNumJson.getAsInt();
    }

    public Long getSqlSelectLimit() {
        return sqlSelectLimit;
    }

    public void setSqlSelectLimit(JsonElement sqlSelectLimit) {
        this.sqlSelectLimit = sqlSelectLimit == null ? SystemConstant.DEFAULT_SQL_SELECT_LIMIT : sqlSelectLimit.getAsLong();
    }

    public Integer getRingbufferSize() {
        return ringbufferSize;
    }

    public void setRingbufferSize(JsonElement jsonElement) {
        this.ringbufferSize = jsonElement == null ? SystemConstant.DEFAULT_RINGBUFFER_SIZE : jsonElement.getAsInt();
    }

    public Integer getParserThreadCount() {
        return parserThreadCount;
    }

    public void setParserThreadCount(JsonElement jsonElement) {
        this.parserThreadCount = jsonElement == null ? SystemConstant.DEFAULT_PARSERTHREAD_COUNT : jsonElement.getAsInt();
    }
}
