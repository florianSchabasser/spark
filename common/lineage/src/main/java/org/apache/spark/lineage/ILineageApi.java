package org.apache.spark.lineage;

public interface ILineageApi {

    // Management
    void register(String nodeId, String name, String description);
    void flowLink(String srcNodeId, String destNodeId);

    // Capture API
    void capture(String key, String flowId, String hashIn, String hashOut, String value);
}
