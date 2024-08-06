package org.apache.spark.lineage;

public interface ILineageApi {

    // Management
    void register(String nodeId, String name, String description);
    void commit(String nodeId);
    void flowLink(String srcNodeId, String destNodeId);

    // Standard Capture API
    void capture(String nodeId, String hashIn, String hashOut);
    void capture(String nodeId, String hashIn, String hashOut, String value);

    // Paired Capture API
    void addInput(String nodeId, String hashIn, String tag);
    void addInput(String nodeId, String hashIn, String tag, String value);
    void addOutput(String nodeId, String hashOut, String tag);
    void addOutput(String nodeId, String hashOut, String tag, String value);
    void reset(String tag);
}
