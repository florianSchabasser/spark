package org.apache.spark.lineage;

import java.util.Map;

public interface ILineageApi {

    void capture(String id, String name, String hashOut, Map<String, String> additionalInformation);

}
