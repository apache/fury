package me.ele.lpd.dispatch.matrix.score.common.dto.eta;

/**
 * ETA数据来源
 */
public enum SourceEnum {
    // 本地计算
    LOCAL,

    /**
     * 从apollo处传下来
     */
    APOLLO,

    // 远程调用ETA服务
    REMOTE,

    // 本地超大距离计算
    LONG_LOCAL,

    // 远程计算超时本地兜底
    ERROR_LOCAL
}
