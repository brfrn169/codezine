package graphdb3;

import graphdb.Direction;
import graphdb.Relationship;
import graphdb2.Filter;
import graphdb2.Sort;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GraphDbService extends graphdb2.GraphDbService {
  // ノードのプロパティのCAS操作
  boolean checkAndUpdateNodeProperties(String nodeId, Map<String, String> expectProperties, Map<String, String> putProperties,
      Set<String> deletePropertyNames) throws IOException;

  // リレーションシップのプロパティのCAS操作
  boolean checkAndUpdateRelationshipProperties(String startNodeId, String type, String endNodeId, Map<String, String> expectProperties,
      Map<String, String> putProperties, Set<String> deletePropertyNames) throws IOException;

  // カーソルをフェッチする
  List<Relationship> fetch(Cursor cursor, int length) throws IOException;

  // カーソルを作成する(最新順)
  Cursor getCursor(String nodeId, String type, Direction direction) throws IOException;

  // カーソルを作成する(セカンダリインデックスを使用)
  Cursor getCursor(String nodeId, String type, Direction direction, Filter filter, Sort sort) throws IOException;
}
