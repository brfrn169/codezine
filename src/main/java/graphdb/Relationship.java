package graphdb;

import java.util.Map;

public class Relationship {
  // リレーションシップの終点ノードID
  private String endNodeId;

  // リレーションシップのプロパティ
  private Map<String, String> properties;

  // リレーションシップの始点ノードID
  private String startNodeId;

  // リレーションシップのタイプ
  private String type;

  public String getEndNodeId() {
    return endNodeId;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public String getStartNodeId() {
    return startNodeId;
  }

  public String getType() {
    return type;
  }

  public void setEndNodeId(String endNodeId) {
    this.endNodeId = endNodeId;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setStartNodeId(String startNodeId) {
    this.startNodeId = startNodeId;
  }

  public void setType(String type) {
    this.type = type;
  }
}
