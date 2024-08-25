package org.apache.spark.lineage;

public interface ILineageApi {

    // Management
    void register(String nodeId, String name, String description);
    void flowLink(String srcNodeId, String destNodeId);

    // Capture API
    void capture(String flowId, String hashIn, String hashOut, String value);
    ILineageApi withName(String name);
    ILineageApi withDescription(String description);

}
