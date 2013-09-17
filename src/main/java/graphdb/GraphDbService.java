package graphdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GraphDbService {
  // ノードの作成
  void createNode(String nodeId, Map<String, String> properties) throws IOException;

  // リレーションシップの作成
  void createRelationship(String startNodeId, String type, String endNodeId, Map<String, String> properties) throws IOException;

  // ノードの削除
  void deleteNode(String nodeId) throws IOException;

  // リレーションシップの削除
  void deleteRelationship(String startNodeId, String type, String endNodeId) throws IOException;

  // ノードのプロパティの取得
  Map<String, String> getNodeProperties(String nodeId) throws IOException;

  // リレーションシップのプロパティの取得
  Map<String, String> getRelationshipProperties(String startNodeId, String type, String endNodeId) throws IOException;

  // 隣接リレーションシップの取得(最新順)
  List<Relationship> select(String nodeId, String type, Direction direction, int length) throws IOException;

  // ノードのプロパティの追加・更新・削除
  void updateNodeProperties(String nodeId, Map<String, String> putProperties, Set<String> deletePropertyNames) throws IOException;

  // リレーションシップのプロパティの追加・更新・削除
  void updateRelationshipProperties(String startNodeId, String type, String endNodeId, Map<String, String> putProperties,
      Set<String> deletePropertyNames) throws IOException;
}
