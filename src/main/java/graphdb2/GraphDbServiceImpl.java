package graphdb2;

import graphdb.Direction;
import graphdb.Relationship;

import java.io.IOException;
import java.nio.ByteBuffer;
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

public class GraphDbServiceImpl extends graphdb.GraphDbServiceImpl implements GraphDbService {

  // リレーションシップのプロパティの最大長(100バイト)
  private static final int RELATIONSHIP_PROPERTY_MAX_LENGTH = 100;

  // コンストラクタ
  public GraphDbServiceImpl(Configuration conf) {
    super(conf);
  }

  // リレーションシップの作成
  @Override
  public void createRelationship(String startNodeId, String type, String endNodeId, Map<String, String> properties) throws IOException {
    HTableInterface table = hTablePool.getTable(TABLE);
    try {

      // ノードの存在チェックなどは省略

      // 作成日時
      long createTimestamp = System.currentTimeMillis();
      byte[] createTimestampBytes = Bytes.toBytes(createTimestamp);

      // プロパティをシリアライズ
      byte[] propertiesBytes = serializeProperty(properties);

      //
      // リレーションシップRowの作成
      //

      // リレーションシップのRowKeyの作成する
      byte[] relationshipRow = createRelationshipRow(startNodeId, type, endNodeId);

      // Putオブジェクトの生成
      Put relationshipPut = new Put(relationshipRow, createTimestamp); // Timestampには作成時間を指定
      relationshipPut.add(COLUMN_FAMILY, PROPERTY_COLUMN, propertiesBytes); // プロパティ
      relationshipPut.add(COLUMN_FAMILY, CREATE_TIMESTAMP_COLUMN, createTimestampBytes); // 作成日時
      relationshipPut.add(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, createTimestampBytes); // 更新日時(最初は作成日時と同じ)

      // checkAndPut。すでにリレーションがある場合はfalseが返ってくる
      boolean success = table.checkAndPut(relationshipRow, COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, null, relationshipPut);
      if (!success) {
        // 既にリレーションシップが存在する場合
        return;
      }

      // 各インデックスを一括でPutするためのリスト
      List<Put> puts = new ArrayList<Put>();

      //
      // リレーションシップの最新順インデックスRowの作成
      //

      // リレーションシップの最新順インデックスのRowKeyを作成する
      List<byte[]> indexRows = createNewOrderIndexRows(startNodeId, type, endNodeId, createTimestamp);

      // Putオブジェクトの生成
      for (byte[] row : indexRows) {
        Put put = new Put(row, createTimestamp); // Timestampには作成時間を指定
        put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */, propertiesBytes); // プロパティ
        puts.add(put);
      }

      //
      // リレーションシップのプロパティのセカンダリインデックスRowの作成
      //
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        // 各プロパティについて
        String propertyName = entry.getKey();
        String propertyValue = entry.getValue();

        // リレーションシップのプロパティのセカンダリインデックスのRowKeyを作成する
        List<byte[]> secondaryIndexRows = createSecondaryIndexRows(startNodeId, type, endNodeId, propertyName, propertyValue,
            createTimestamp);

        for (byte[] row : secondaryIndexRows) {
          Put put = new Put(row, createTimestamp); // Timestampには作成時間を指定
          put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */, propertiesBytes); // プロパティ
          puts.add(put);
        }
      }

      // バッチ処理で一括でPut
      table.batch(puts);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // リレーションシップの削除
  @Override
  public void deleteRelationship(String startNodeId, String type, String endNodeId) throws IOException {
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      //
      // リレーションシップRowの削除
      //

      // リレーションシップのRowKeyの作成する
      byte[] relationshipRow = createRelationshipRow(startNodeId, type, endNodeId);

      // 削除する時間
      long deleteTimestamp;

      // 削除するリレーションシップの作成時間
      long createTimestamp;

      // 削除するリレーションシップのプロパティ
      Map<String, String> properties;

      while (true) {
        // Getオブジェクトの生成
        Get get = new Get(relationshipRow);
        get.addFamily(COLUMN_FAMILY);

        // 結果を取得
        Result result = table.get(get);

        if (result.isEmpty()) {
          // リレーションシップが存在しない場合
          return;
        }

        // 削除するリレーションシップのプロパティ
        properties = deserializeProperty(result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN));

        // 削除するリレーションシップの作成時間
        createTimestamp = Bytes.toLong(result.getValue(COLUMN_FAMILY, CREATE_TIMESTAMP_COLUMN));

        // 更新時間
        byte[] lastUpdateTimestampBytes = result.getValue(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN);
        long lastUpdateTimestamp = Bytes.toLong(lastUpdateTimestampBytes);

        // 削除時間
        deleteTimestamp = System.currentTimeMillis();
        if (deleteTimestamp <= lastUpdateTimestamp) {
          // 削除時間の単調増加を保証するため
          deleteTimestamp = lastUpdateTimestamp + 1;
        }

        // Deleteオブジェクトの生成
        Delete delete = new Delete(relationshipRow, deleteTimestamp); // Timestampには削除時間を指定

        // checkAndDelete。falseが返ってきたらもう一度繰り返す
        boolean success = table.checkAndDelete(relationshipRow, COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, lastUpdateTimestampBytes, delete);
        if (success) {
          break;
        }
      }

      // 各インデックスを一括でDeleteするためのリスト
      List<Delete> deletes = new ArrayList<Delete>();

      //
      // リレーションシップの最新順インデックスRowの削除
      //

      // リレーションシップの最新順インデックスのRowKeyを作成する
      List<byte[]> indexRows = createNewOrderIndexRows(startNodeId, type, endNodeId, deleteTimestamp);

      // Deleteオブジェクトの生成
      for (byte[] row : indexRows) {
        Delete delete = new Delete(row, deleteTimestamp); // Timestampには削除時間を指定
        deletes.add(delete);
      }

      //
      // リレーションシップのプロパティのセカンダリインデックスRowの削除
      //
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        // 各プロパティについて
        String propertyName = entry.getKey();
        String propertyValue = entry.getValue();

        // リレーションシップのプロパティのセカンダリインデックスのRowKeyを作成する
        List<byte[]> secondaryIndexRows = createSecondaryIndexRows(startNodeId, type, endNodeId, propertyName, propertyValue,
            createTimestamp);

        for (byte[] row : secondaryIndexRows) {
          Delete delete = new Delete(row, deleteTimestamp); // Timestampには削除時間を指定
          deletes.add(delete);
        }
      }

      // バッチ処理で一括でDelete
      table.batch(deletes);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // 隣接リレーションシップの取得(セカンダリインデックスを使用)
  @Override
  public List<Relationship> select(String nodeId, String type, Direction direction, Filter filter, Sort sort, int length)
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

    // Scanオブジェクトを生成する
    Scan scan = createSecondaryIndexScan(nodeId, type, direction, propertyName, filter, order);
    scan.addFamily(COLUMN_FAMILY);

    HTableInterface table = hTablePool.getTable(TABLE);
    ResultScanner scanner = null;
    try {
      // セカンダリインデックスの作成時の整合性の問題で、同じノードIDが複数取得できてしまう可能性があるので、それを除外するためのSet
      Set<String> nodeIdSet = new HashSet<String>();

      scanner = table.getScanner(scan);
      // Scanして結果を取得する
      List<Relationship> ret = new ArrayList<Relationship>();
      for (Result result : scanner) {
        String id = extractNodeIdFromSecondaryIndexRow(result.getRow());
        if (nodeIdSet.contains(id)) {
          // 既に結果に入っているノードIDはスキップする
          continue;
        }
        nodeIdSet.add(id);

        Relationship relationship = new Relationship();
        ret.add(relationship);
        relationship.setType(type);
        relationship.setProperties(deserializeProperty(result.getValue(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */))); // プロパティを取得してセット

        switch (direction) {
        case INCOMING:
          relationship.setStartNodeId(id); // RowKeyからノードIDを取得
          relationship.setEndNodeId(nodeId);
          break;
        case OUTGOING:
          relationship.setStartNodeId(nodeId);
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

      return ret;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      table.close();
    }
  }

  // リレーションシップのプロパティの追加・更新・削除
  @Override
  public void updateRelationshipProperties(String startNodeId, String type, String endNodeId, Map<String, String> putProperties,
      Set<String> deletePropertyNames) throws IOException {
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
          return;
        }
        // 作成時間
        createTimestamp = Bytes.toLong(result.getValue(COLUMN_FAMILY, CREATE_TIMESTAMP_COLUMN));

        // 現在の更新時間
        byte[] lastUpdateTimestampBytes = result.getValue(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN);
        long lastUpdateTimestamp = Bytes.toLong(lastUpdateTimestampBytes);

        // 現在のプロパティ
        lastProperties = deserializeProperty(result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN));

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
          List<byte[]> putSecondaryIndexRows = createSecondaryIndexRows(startNodeId, type, endNodeId, propertyName,
              propertyValue, createTimestamp);
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
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // リレーションシップのプロパティによるセカンダリインデックスのRowKeyの作成。方向(INCOMING,OUTGOING) × 順序(ASC,DESC)の4つ
  protected List<byte[]> createSecondaryIndexRows(String startNodeId, String type, String endNodeId, String propertyName,
      String propertyValue, long createTimestamp) {
    byte[] startNodeIdBytes = Bytes.toBytes(startNodeId);
    byte[] typeBytes = Bytes.toBytes(type);
    byte[] endNodeIdBytes = Bytes.toBytes(endNodeId);
    long reverseTimestamp = Long.MAX_VALUE - createTimestamp;

    byte[] propertyNameBytes = Bytes.toBytes(propertyName);
    byte[] propertyValueBytes = padBytes(Bytes.toBytes(propertyValue));
    byte[] invertedPropertyValueBytes = invertBytes(padBytes(Bytes.toBytes(propertyValue)));

    List<byte[]> rows = new ArrayList<byte[]>();

    ByteBuffer buffer;

    // INCOMING,ASC
    buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + endNodeIdBytes.length // int型 + endNodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 4 + propertyNameBytes.length // int型 + typeのバイト配列
        + 1 // byte型
        + RELATIONSHIP_PROPERTY_MAX_LENGTH // 100バイト
        + 8 // long型
        + 4 + startNodeIdBytes.length // int型 + startNodeIdのバイト配列
    );
    buffer.putInt(endNodeId.hashCode()) // hash
        .put((byte) 3) // 3
        .putInt(endNodeIdBytes.length) // endNodeIdのバイト数
        .put(endNodeIdBytes) // endNodeIdのバイト配列
        .put(getDirectionByte(Direction.INCOMING)) // IMCOMING
        .putInt(typeBytes.length) // typeのバイト数
        .put(typeBytes) // typeのバイト配列
        .putInt(propertyNameBytes.length) // propertyNameのバイト数
        .put(propertyNameBytes) // propertyNameのバイト配列
        .put(getOrderByte(Order.ASC)) // ASC
        .put(propertyValueBytes) // propertyValueのバイト配列(100バイト)
        .putLong(reverseTimestamp) // Long.MAX_VALUE - createTimestamp
        .putInt(startNodeIdBytes.length) // startNodeIdのバイト数
        .put(startNodeIdBytes); // startNodeIdのバイト配列

    rows.add(buffer.array());

    // INCOMING,DESC
    buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + endNodeIdBytes.length // int型 + endNodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 4 + propertyNameBytes.length // int型 + typeのバイト配列
        + 1 // byte型
        + RELATIONSHIP_PROPERTY_MAX_LENGTH // 100バイト
        + 8 // long型
        + 4 + startNodeIdBytes.length // int型 + startNodeIdのバイト配列
    );
    buffer.putInt(endNodeId.hashCode()) // hash
        .put((byte) 3) // 3
        .putInt(endNodeIdBytes.length) // endNodeIdのバイト数
        .put(endNodeIdBytes) // endNodeIdのバイト配列
        .put(getDirectionByte(Direction.INCOMING)) // IMCOMING
        .putInt(typeBytes.length) // typeのバイト数
        .put(typeBytes) // typeのバイト配列
        .putInt(propertyNameBytes.length) // propertyNameのバイト数
        .put(propertyNameBytes) // propertyNameのバイト配列
        .put(getOrderByte(Order.DESC)) // DESC
        .put(invertedPropertyValueBytes) // propertyValueのバイト配列をビット反転したもの(100バイト)
        .putLong(reverseTimestamp) // Long.MAX_VALUE - createTimestamp
        .putInt(startNodeIdBytes.length) // startNodeIdのバイト数
        .put(startNodeIdBytes); // startNodeIdのバイト配列

    rows.add(buffer.array());

    // OUTGOING,ASC
    buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + startNodeIdBytes.length // int型 + startNodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 4 + propertyNameBytes.length // int型 + typeのバイト配列
        + 1 // byte型
        + RELATIONSHIP_PROPERTY_MAX_LENGTH // 100バイト
        + 8 // long型
        + 4 + endNodeIdBytes.length // int型 + endNodeIdのバイト配列
    );
    buffer.putInt(startNodeId.hashCode()) // hash
        .put((byte) 3) // 3
        .putInt(startNodeIdBytes.length) // startNodeIdのバイト数
        .put(startNodeIdBytes) // startNodeIdのバイト配列
        .put(getDirectionByte(Direction.OUTGOING)) // OUTGOING
        .putInt(typeBytes.length) // typeのバイト数
        .put(typeBytes) // typeのバイト配列
        .putInt(propertyNameBytes.length) // propertyNameのバイト数
        .put(propertyNameBytes) // propertyNameのバイト配列
        .put(getOrderByte(Order.ASC)) // ASC
        .put(propertyValueBytes) // propertyValueのバイト配列(100バイト)
        .putLong(reverseTimestamp) // Long.MAX_VALUE - createTimestamp
        .putInt(endNodeIdBytes.length) // endNodeIdのバイト数
        .put(endNodeIdBytes); // endNodeIdのバイト配列

    rows.add(buffer.array());

    // OUTGOING,DESC
    buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + startNodeIdBytes.length // int型 + startNodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 4 + propertyNameBytes.length // int型 + typeのバイト配列
        + 1 // byte型
        + RELATIONSHIP_PROPERTY_MAX_LENGTH // 100バイト
        + 8 // long型
        + 4 + endNodeIdBytes.length // int型 + endNodeIdのバイト配列
    );
    buffer.putInt(startNodeId.hashCode()) // hash
        .put((byte) 3) // 3
        .putInt(startNodeIdBytes.length) // startNodeIdのバイト数
        .put(startNodeIdBytes) // startNodeIdのバイト配列
        .put(getDirectionByte(Direction.OUTGOING)) // OUTGOING
        .putInt(typeBytes.length) // typeのバイト数
        .put(typeBytes) // typeのバイト配列
        .putInt(propertyNameBytes.length) // propertyNameのバイト数
        .put(propertyNameBytes) // propertyNameのバイト配列
        .put(getOrderByte(Order.DESC)) // DESC
        .put(invertedPropertyValueBytes) // propertyValueのバイト配列をビット反転したもの(100バイト)
        .putLong(reverseTimestamp) // Long.MAX_VALUE - createTimestamp
        .putInt(endNodeIdBytes.length) // endNodeIdのバイト数
        .put(endNodeIdBytes); // endNodeIdのバイト配列

    rows.add(buffer.array());

    return rows;
  }

  // セカンダリインデックスをScanするためのRowの作成
  private byte[] createSecondaryIndexScanRow(String nodeId, String type, Direction direction, String propertyName, Order order) {
    byte[] nodeIdBytes = Bytes.toBytes(nodeId);
    byte[] typeBytes = Bytes.toBytes(type);
    byte[] propertyNameBytes = Bytes.toBytes(propertyName);

    ByteBuffer buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + nodeIdBytes.length // int型 + nodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 4 + propertyNameBytes.length // int型 + typeのバイト配列
        + 1 // byte型
    );
    buffer.putInt(nodeId.hashCode()) // hash
        .put((byte) 3) // 3
        .putInt(nodeIdBytes.length) // nodeIdのバイト数
        .put(nodeIdBytes) // nodeIdのバイト配列
        .put(getDirectionByte(direction)) // direction
        .putInt(typeBytes.length) // typeのバイト数
        .put(typeBytes) // typeのバイト配列
        .putInt(propertyNameBytes.length) // propertyNameのバイト数
        .put(propertyNameBytes) // propertyNameのバイト配列
        .put(getOrderByte(order)); // desc or asc

    return buffer.array();
  }

  // セカンダリインデックスをScanするためのRowの作成
  private byte[] createSecondaryIndexScanRow(String nodeId, String type, Direction direction, String propertyName,
      String propertyValue, Order order) {
    byte[] nodeIdBytes = Bytes.toBytes(nodeId);
    byte[] typeBytes = Bytes.toBytes(type);
    byte[] propertyNameBytes = Bytes.toBytes(propertyName);

    byte[] propertyValueBytes;
    if (order == Order.ASC) {
      propertyValueBytes = padBytes(Bytes.toBytes(propertyValue));
    } else { // order == Order.DESC
      // ビット反転
      propertyValueBytes = invertBytes(padBytes(Bytes.toBytes(propertyValue)));
    }

    ByteBuffer buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + nodeIdBytes.length // int型 + nodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 4 + propertyNameBytes.length // int型 + typeのバイト配列
        + 1 // byte型
        + RELATIONSHIP_PROPERTY_MAX_LENGTH // 100バイト
    );
    buffer.putInt(nodeId.hashCode()) // hash
        .put((byte) 3) // 3
        .putInt(nodeIdBytes.length) // nodeIdのバイト数
        .put(nodeIdBytes) // nodeIdのバイト配列
        .put(getDirectionByte(direction)) // direction
        .putInt(typeBytes.length) // typeのバイト数
        .put(typeBytes) // typeのバイト配列
        .putInt(propertyNameBytes.length) // propertyNameのバイト数
        .put(propertyNameBytes) // propertyNameのバイト配列
        .put(getOrderByte(order)) // asc or desc
        .put(propertyValueBytes); // propertyValueのバイト配列

    return buffer.array();
  }

  // FilterやSortの条件からScanを生成する
  protected Scan createSecondaryIndexScan(String nodeId, String type, Direction direction, String propertyName, Filter filter, Order order) {
    byte[] startRow;
    byte[] stopRow;

    if (filter != null) {
      String value = filter.getValue();

      switch (filter.getOperator()) {
      case EQUAL:
        // EQUALの場合は、Sortに関わらずASCのセカンダリインデックスを使用する
        startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.ASC);
        stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.ASC));
        break;

      case GREATER:
        if (order == Order.ASC) {
          startRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.ASC));
          stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.ASC));
        } else { // order == Order.DESC
          startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.DESC);
          stopRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.DESC);
        }
        break;

      case GREATER_OR_EQUAL:
        if (order == Order.ASC) {
          startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.ASC);
          stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.ASC));
        } else { // order == Order.DESC
          startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.DESC);
          stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.DESC));
        }
        break;

      case LESS:
        if (order == Order.ASC) {
          startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.ASC);
          stopRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.ASC);
        } else { // order == Order.DESC
          startRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.DESC));
          stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.DESC));
        }
        break;

      case LESS_OR_EQUAL:
        if (order == Order.ASC) {
          startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.ASC);
          stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.ASC));
        } else { // order == Order.DESC
          startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, value, Order.DESC);
          stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, Order.DESC));
        }
        break;

      default:
        throw new AssertionError();
      }
    } else {
      startRow = createSecondaryIndexScanRow(nodeId, type, direction, propertyName, order);
      stopRow = incrementBytes(createSecondaryIndexScanRow(nodeId, type, direction, propertyName, order));
    }
    return new Scan(startRow, stopRow);
  }

  // セカンダリインデックスのRowからノードIDを抽出する
  protected String extractNodeIdFromSecondaryIndexRow(byte[] row) {
    ByteBuffer buffer = ByteBuffer.wrap(row);

    byte[] bytes;

    buffer.getInt(); // hash
    buffer.get(); // 3

    // nodeId
    bytes = new byte[buffer.getInt()];
    buffer.get(bytes);

    buffer.get(); // direction

    // type
    bytes = new byte[buffer.getInt()];
    buffer.get(bytes);

    // propertyName
    bytes = new byte[buffer.getInt()];
    buffer.get(bytes);

    buffer.get(); // asc or desc

    // propertyValue
    bytes = new byte[RELATIONSHIP_PROPERTY_MAX_LENGTH];
    buffer.get(bytes);

    buffer.getLong(); // reverse timestamp

    // nodeId
    bytes = new byte[buffer.getInt()];
    buffer.get(bytes);

    return Bytes.toString(bytes);
  }

  // 順序のバイト表現を返す(ASCは1,DESCは2)
  private byte getOrderByte(Order order) {
    switch (order) {
    case ASC:
      return (byte) 1;
    case DESC:
      return (byte) 2;
    default:
      throw new AssertionError();
    }
  }

  // バイト配列をビット反転させる
  private byte[] invertBytes(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.allocate(bytes.length);

    for (int i = 0; i < bytes.length; i++) {
      buffer.put((byte) ~bytes[i]);
    }
    return buffer.array();
  }

  // RELATIONSHIP_PROPERTY_MAX_LENGTHバイトまで0で埋めたバイト配列を返す
  private byte[] padBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(RELATIONSHIP_PROPERTY_MAX_LENGTH);
    buffer.put(bytes);
    return buffer.array();
  }
}
