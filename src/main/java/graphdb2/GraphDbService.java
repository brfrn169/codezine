package graphdb2;

import graphdb.Direction;
import graphdb.Relationship;

import java.io.IOException;
import java.util.List;

public interface GraphDbService extends graphdb.GraphDbService {
  // 隣接リレーションシップの取得(セカンダリインデックスを使用)
  List<Relationship> select(String nodeId, String type, Direction direction, Filter filter, Sort sort, int length) throws IOException;
}
