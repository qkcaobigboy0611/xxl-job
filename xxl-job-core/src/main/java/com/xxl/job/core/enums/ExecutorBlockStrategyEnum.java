package com.xxl.job.core.enums;

/**
 * 阻塞处理策略，  有单行串行，丢弃后续调度，覆盖之前调度
 */
public enum ExecutorBlockStrategyEnum {

    // 单行串行
    SERIAL_EXECUTION("Serial execution"),
    // 丢弃后续调度
    DISCARD_LATER("Discard Later"),
    // 覆盖之前的调度
    COVER_EARLY("Cover Early");
    // 自己系统中多了一个集群丢弃后续调度

    private String title;
    private ExecutorBlockStrategyEnum (String title) {
        this.title = title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    public String getTitle() {
        return title;
    }

    public static ExecutorBlockStrategyEnum match(String name, ExecutorBlockStrategyEnum defaultItem) {
        if (name != null) {
            for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }
}
