package graphdb3;

import graphdb.Direction;
import graphdb.Relationship;
import graphdb2.Filter;
import graphdb2.Order;
import graphdb2.Sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class GraphDbServiceImpl extends graphdb2.GraphDbServiceImpl implements GraphDbService {
  public GraphDbServiceImpl(Configuration conf) {
    super(conf);
  }

  // ノードのプロパティのCAS操作
  @Override
  public boolean checkAndUpdateNodeProperties(String nodeId, Map<String, String> expectProperties,
      Map<String, String> putProperties, Set<String> deletePropertyNames) throws IOException {
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      // ノードのRowKeyを作成する
      byte[] row = createNodeRow(nodeId);

      while (true) {
        // Getオブジェクトの生成
        Get get = new Get(row);
        get.addFamily(COLUMN_FAMILY);

        // 結果を取得
        Result result = table.get(get);

        if (result.isEmpty()) {
          // ノードが存在しない場合
          return false;
        }

        // 現在の更新時間
        byte[] lastUpdateTimestampBytes = result.getValue(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN);
        long lastUpdateTimestamp = Bytes.toLong(lastUpdateTimestampBytes);

        // 現在のプロパティ
        Map<String, String> lastProperties = deserializeProperty(result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN));

        // check
        for (Map.Entry<String, String> prop : expectProperties.entrySet()) {
          if (!lastProperties.get(prop.getKey()).equals(prop.getValue())) {
            return false;
          }
        }

        // 新しい更新時間
        long updateTimestamp = System.currentTimeMillis();
        if (updateTimestamp <= lastUpdateTimestamp) {
          updateTimestamp = lastUpdateTimestamp + 1;
        }

        // 新しいプロパティ
        Map<String, String> properties = new HashMap<String, String>(lastProperties);
        // 更新・追加
        if (putProperties != null) {
          properties.putAll(putProperties);
        }
        // 削除
        if (deletePropertyNames != null) {
          for (String name : deletePropertyNames) {
            properties.remove(name);
          }
        }
        byte[] propertiesBytes = serializeProperty(properties);

        // Putオブジェクトの生成
        Put put = new Put(row, updateTimestamp); // Timestampには更新時間を指定
        put.add(COLUMN_FAMILY, PROPERTY_COLUMN, propertiesBytes);
        put.add(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, Bytes.toBytes(updateTimestamp));

        // checkAndPut。falseが返ってきたら、もう一度繰り返す
        boolean success = table.checkAndPut(row, COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, lastUpdateTimestampBytes, put);
        if (success) {
          return true;
        }
      }
    } finally {
      table.close();
    }
  }

  // リレーションシップのプロパティのCAS操作
  @Override
  public boolean checkAndUpdateRelationshipProperties(String startNodeId, String type, String endNodeId,
      Map<String, String> expectProperties, Map<String, String> putProperties, Set<String> deletePropertyNames)
      throws IOException {
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      //
      // リレーションシップRowの更新
      //

      // リレーションシップのRowKeyの作成する
      byte[] relationshipRow = createRelationshipRow(startNodeId, type, endNodeId);

      // 作成時間
      long createTimestamp;

      // 現在のプロパティ
      Map<String, String> lastProperties;

      // 新しいの更新時間
      long updateTimestamp;

      // 新しいのプロパティ
      byte[] propertiesBytes;

      while (true) {
        // Getオブジェクトの生成
        Get get = new Get(relationshipRow);
        get.addFamily(COLUMN_FAMILY);

        // 結果を取得
        Result result = table.get(get);

        if (result.isEmpty()) {
          // リレーションシップが存在しない場合
          return false;
        }
        // 作成時間
        createTimestamp = Bytes.toLong(result.getValue(COLUMN_FAMILY, CREATE_TIMESTAMP_COLUMN));

        // 現在の更新時間
        byte[] lastUpdateTimestampBytes = result.getValue(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN);
        long lastUpdateTimestamp = Bytes.toLong(lastUpdateTimestampBytes);

        // 現在のプロパティ
        lastProperties = deserializeProperty(result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN));

        // check
        for (Map.Entry<String, String> prop : expectProperties.entrySet()) {
          if (!lastProperties.get(prop.getKey()).equals(prop.getValue())) {
            return false;
          }
        }

        // 新しい更新時間
        updateTimestamp = System.currentTimeMillis();
        if (updateTimestamp <= lastUpdateTimestamp) {
          // 更新時間の単調増加を保証するため
          updateTimestamp = lastUpdateTimestamp + 1;
        }

        // 新しいプロパティ
        Map<String, String> properties = new HashMap<String, String>(lastProperties);
        // 更新・追加
        if (putProperties != null) {
          properties.putAll(putProperties);
        }
        // 削除
        if (deletePropertyNames != null) {
          for (String name : deletePropertyNames) {
            properties.remove(name);
          }
        }
        propertiesBytes = serializeProperty(properties);

        // Putオブジェクトの生成
        Put put = new Put(relationshipRow, updateTimestamp); // Timestampには更新時間を指定
        put.add(COLUMN_FAMILY, PROPERTY_COLUMN, propertiesBytes);
        put.add(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, Bytes.toBytes(updateTimestamp));

        // checkAndPut
        boolean success = table.checkAndPut(relationshipRow, COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, lastUpdateTimestampBytes, put);
        if (success) {
          break;
        }
      }

      // 各インデックスを一括でPut,Deleteするためのリスト
      List<Put> puts = new ArrayList<Put>();
      List<Delete> deletes = new ArrayList<Delete>();

      //
      // リレーションシップの最新順インデックスRowの削除
      //

      // リレーションシップの最新順インデックスのRowKeyを作成する
      List<byte[]> indexRows = createNewOrderIndexRows(startNodeId, type, endNodeId, createTimestamp);

      // Putオブジェクトの生成
      for (byte[] row : indexRows) {
        Put put = new Put(row, updateTimestamp); // Timestampには更新時間を指定
        put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */, propertiesBytes);
        puts.add(put);
      }

      //
      // リレーションシップのプロパティのセカンダリインデックスRowの更新
      //

      if (putProperties != null) {
        for (Map.Entry<String, String> entry : putProperties.entrySet()) {
          // 各プロパティについて
          String propertyName = entry.getKey();
          String propertyValue = entry.getValue();

          // PutするリレーションシップのプロパティのセカンダリインデックスのRowKeyを作成する
          List<byte[]> putSecondaryIndexRows = createSecondaryIndexRows(startNodeId, type, endNodeId, propertyName, propertyValue,
              createTimestamp);
          for (byte[] row : putSecondaryIndexRows) {
            Put put = new Put(row, updateTimestamp); // Timestampには更新時間を指定
            put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */, propertiesBytes);
            puts.add(put);
          }

          // Deleteする古いリレーションシップのプロパティのセカンダリインデックスのRowKeyを作成する
          List<byte[]> deleteSecondaryIndexRows = createSecondaryIndexRows(startNodeId, type, endNodeId, propertyName,
              lastProperties.get(propertyName), createTimestamp);
          for (byte[] row : deleteSecondaryIndexRows) {
            Delete delete = new Delete(row, updateTimestamp); // Timestampには更新時間を指定
            deletes.add(delete);
          }
        }
      }

      if (deletePropertyNames != null) {
        for (String propertyName : deletePropertyNames) {
          // 各プロパティについて

          // Deleteする古いリレーションシップのプロパティのセカンダリインデックスのRowKeyを作成する
          List<byte[]> deleteSecondaryIndexRows = createSecondaryIndexRows(startNodeId, type, endNodeId, propertyName,
              lastProperties.get(propertyName), createTimestamp);
          for (byte[] row : deleteSecondaryIndexRows) {
            Delete delete = new Delete(row, updateTimestamp); // Timestampには更新時間を指定
            deletes.add(delete);
          }
        }
      }

      // バッチ処理で一括でPutしてからDelete(順序が重要)
      if (!puts.isEmpty()) {
        table.batch(puts);
      }
      if (!deletes.isEmpty()) {
        table.batch(deletes);
      }
      return true;
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // カーソルをフェッチする
  @Override
  public List<Relationship> fetch(Cursor cursor, int length) throws IOException {
    switch (cursor.getCursorType()) {
    case NEW_ORDER_INDEX:
      // 最新順で取得する場合
      return fetchNewOrderIndex(cursor, length);
    case SECONDARY_INDEX:
      // セカンダリインデックスを使用する場合
      return fetchSecondaryIndex(cursor, length);
    default:
      throw new AssertionError();
    }
  }

  // カーソルを作成する(最新順)
  @Override
  public Cursor getCursor(String nodeId, String type, Direction direction) throws IOException {
    // startRow
    byte[] startRow = createNowOrderIndexScanStartRow(nodeId, type, direction);

    // startRowをインクリメントしたものをstopRowとする
    byte[] stopRow = createNowOrderIndexScanStartRow(nodeId, type, direction);
    incrementBytes(stopRow); // バイト配列をインクリメントする

    // カーソルオブジェクトの作成
    Cursor cursor = new Cursor();
    cursor.setStartRow(startRow);
    cursor.setStopRow(stopRow);
    cursor.setCursorType(Cursor.CursorType.NEW_ORDER_INDEX);
    cursor.setNodeId(nodeId);
    cursor.setType(type);
    cursor.setDirection(direction);
    return cursor;
  }

  // カーソルを作成する(セカンダリインデックスを使用)
  @Override
  public Cursor getCursor(String nodeId, String type, Direction direction, Filter filter, Sort sort)
      throws IOException {
    // プロパティ名
    String propertyName;
    if (filter != null) {
      propertyName = filter.getPropertyName();
    } else {
      propertyName = sort.getPropertyName();
    }

    // 順序
    Order order = Order.ASC; // Sortが指定されてなければ昇順
    if (sort != null) {
      order = sort.getOrder();
    }

    // Scanオブジェクトを生成する(セカンダリインデックスを使用)
    Scan scan = createSecondaryIndexScan(nodeId, type, direction, propertyName, filter, order);

    // カーソルオブジェクトを作成
    Cursor cursor = new Cursor();
    cursor.setStartRow(scan.getStartRow());
    cursor.setStopRow(scan.getStopRow());
    cursor.setCursorType(Cursor.CursorType.SECONDARY_INDEX);
    cursor.setNodeId(nodeId);
    cursor.setType(type);
    cursor.setDirection(direction);
    return cursor;
  }

  // カーソルをフェッチする(最新順)
  private List<Relationship> fetchNewOrderIndex(Cursor cursor, int length) throws IOException {
    // Scanオブジェクトを生成する
    Scan scan = new Scan(cursor.getStartRow(), cursor.getStopRow());
    scan.addFamily(COLUMN_FAMILY);

    byte[] lastRow = null;

    HTableInterface table = hTablePool.getTable(TABLE);
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);

      // Scanして結果を取得する
      List<Relationship> ret = new ArrayList<Relationship>();
      for (Result result : scanner) {
        lastRow = result.getRow();

        Relationship relationship = new Relationship();
        ret.add(relationship);

        relationship.setType(cursor.getType());
        relationship.setProperties(deserializeProperty(result.getValue(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */))); // プロパティを取得してセット

        switch (cursor.getDirection()) {
        case INCOMING:
          relationship.setStartNodeId(extractNodeIdFromNewOrderIndexRow(result.getRow())); // RowKeyからノードIDを取得
          relationship.setEndNodeId(cursor.getNodeId());
          break;
        case OUTGOING:
          relationship.setStartNodeId(cursor.getNodeId());
          relationship.setEndNodeId(extractNodeIdFromNewOrderIndexRow(result.getRow())); // RowKeyからノードIDを取得
          break;
        default:
          throw new AssertionError();
        }

        // length件取得できたら終了
        if (ret.size() == length) {
          break;
        }
      }

      if (lastRow != null) {
        // 最後に取得できたRowをインクリメントしたバイト配列を次のScanのstartRowとして使う
        cursor.setStartRow(incrementBytes(lastRow));
      }
      return ret;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      table.close();
    }
  }

  // カーソルをフェッチする(セカンダリインデックスを使用)
  private List<Relationship> fetchSecondaryIndex(Cursor cursor, int length) throws IOException {
    // Scanオブジェクトを生成する
    Scan scan = new Scan(cursor.getStartRow(), cursor.getStopRow());
    scan.addFamily(COLUMN_FAMILY);

    byte[] lastRow = null;

    HTableInterface table = hTablePool.getTable(TABLE);
    ResultScanner scanner = null;
    try {
      // セカンダリインデックスの作成時の整合性の問題で、同じノードIDが複数取得できてしまう可能性があるので、それを除外するためのSet
      Set<String> nodeIdSet = new HashSet<String>();

      scanner = table.getScanner(scan);
      // Scanして結果を取得する
      List<Relationship> ret = new ArrayList<Relationship>();
      for (Result result : scanner) {
        lastRow = result.getRow();

        String id = extractNodeIdFromSecondaryIndexRow(result.getRow());
        if (nodeIdSet.contains(id)) {
          // 既に結果に入っているノードIDはスキップする
          continue;
        }
        nodeIdSet.add(id);

        Relationship relationship = new Relationship();
        ret.add(relationship);
        relationship.setType(cursor.getType());
        relationship.setProperties(deserializeProperty(result.getValue(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */))); // プロパティを取得してセット

        switch (cursor.getDirection()) {
        case INCOMING:
          relationship.setStartNodeId(id); // RowKeyからノードIDを取得
          relationship.setEndNodeId(cursor.getNodeId());
          break;
        case OUTGOING:
          relationship.setStartNodeId(cursor.getNodeId());
          relationship.setEndNodeId(id); // RowKeyからノードIDを取得
          break;
        default:
          throw new AssertionError();
        }

        // length件取得できたら終了
        if (ret.size() == length) {
          break;
        }
      }

      if (lastRow != null) {
        // 最後に取得できたRowをインクリメントしたバイト配列を次のScanのstartRowとして使う
        cursor.setStartRow(incrementBytes(lastRow));
      }
      return ret;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      table.close();
    }
  }
}
