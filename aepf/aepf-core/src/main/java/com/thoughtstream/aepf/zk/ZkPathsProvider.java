package com.thoughtstream.aepf.zk;

/**
 * @author Sateesh Pinnamaneni
 * @since 11/01/2018
 */
public class ZkPathsProvider {
    final static String WATERMARK_STR = "watermarks";
    final static String QUEUES_STR = "queues";
    final static String LOCK_STR = "-lock";

    private final String zookeeperRoot;
    private final String applicationId;

    public ZkPathsProvider(String zookeeperRoot, String applicationId) {
        this.zookeeperRoot = zookeeperRoot;
        this.applicationId = applicationId;
    }

    public String getApplicationPath() {
        return zookeeperRoot + "/" + applicationId;
    }

    public String getWatermarksPath() {
        return getApplicationPath() + "/" + WATERMARK_STR;
    }

    public String getWatermarkPath(String eventSourceId) {
        return getWatermarksPath() + "/" + eventSourceId;
    }

    public String getWatermarkLockPath(String eventSourceId) {
        return getWatermarkPath(eventSourceId) + LOCK_STR;
    }

    public String getOutstandingTasksQueuePath() {
        return getApplicationPath() + "/" + QUEUES_STR + "/outstanding-tasks";
    }

    public String getOutstandingTasksQueueLockPath() {
        return getOutstandingTasksQueuePath() + LOCK_STR;
    }

    public String getCompletedTasksQueuePath() {
        return getApplicationPath() + "/" + QUEUES_STR + "/completed-tasks";
    }

    public String getCompletedTasksQueueLockPath() {
        return getCompletedTasksQueuePath() + LOCK_STR;
    }
}
