package graphdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class GraphDbServiceImpl implements GraphDbService {
  // Table名
  protected static final String TABLE = "graph";

  // ColumnFamily名
  protected static final byte[] COLUMN_FAMILY = Bytes.toBytes("g");

  // Column名
  protected static final byte[] PROPERTY_COLUMN = Bytes.toBytes("p");
  protected static final byte[] UPDATE_TIMESTAMP_COLUMN = Bytes.toBytes("u");
  protected static final byte[] CREATE_TIMESTAMP_COLUMN = Bytes.toBytes("c");

  protected final HTablePool hTablePool;

  // コンストラクタ
  public GraphDbServiceImpl(Configuration conf) {
    hTablePool = new HTablePool(conf, Integer.MAX_VALUE);
  }

  // ノードの作成
  @Override
  public void createNode(String nodeId, Map<String, String> properties) throws IOException {
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      // ノードのRowKeyを作成する
      byte[] row = createNodeRow(nodeId);

      // 作成日時
      long createTimestamp = System.currentTimeMillis();
      byte[] createTimestampBytes = Bytes.toBytes(createTimestamp);

      // プロパティをシリアライズ
      byte[] propertiesBytes = serializeProperty(properties);

      // Putオブジェクトの生成
      Put put = new Put(row, createTimestamp);
      put.add(COLUMN_FAMILY, PROPERTY_COLUMN, propertiesBytes); // プロパティ
      put.add(COLUMN_FAMILY, CREATE_TIMESTAMP_COLUMN, createTimestampBytes); // 作成日時
      put.add(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, createTimestampBytes); // 更新日時(最初は作成日時と同じ)

      // checkAndPut。すでにノードがある場合はfalseが返ってくる
      boolean success = table.checkAndPut(row, COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, null, put);
      if (!success) {
        // 既にノードが存在する場合
        return;
      }
    } finally {
      table.close();
    }
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

      //
      // リレーションシップの最新順インデックスRowの作成
      //
      List<Put> indexPuts = new ArrayList<Put>();

      // リレーションシップの最新順インデックスのRowKeyを作成する
      List<byte[]> indexRows = createNewOrderIndexRows(startNodeId, type, endNodeId, createTimestamp);

      // Putオブジェクトの生成
      for (byte[] row : indexRows) {
        Put put = new Put(row, createTimestamp); // Timestampには作成時間を指定
        put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */, propertiesBytes); // プロパティ
        indexPuts.add(put);
      }

      // バッチ処理でPut
      table.batch(indexPuts);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // ノードの削除
  @Override
  public void deleteNode(String nodeId) throws IOException {
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
          return;
        }

        // 現在の更新時間
        byte[] updateTimestampBytes = result.getValue(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN);
        long updateTimestamp = Bytes.toLong(updateTimestampBytes);

        // 削除時間
        long deleteTimestamp = System.currentTimeMillis();
        if (deleteTimestamp <= updateTimestamp) {
          // 削除時間の単調増加を保証するため
          deleteTimestamp = updateTimestamp + 1;
        }

        // Deleteオブジェクトの生成
        Delete delete = new Delete(row, deleteTimestamp);

        // checkAndDelete。falseが返ってきたらもう一度繰り返す
        boolean success = table.checkAndDelete(row, COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, updateTimestampBytes, delete);
        if (success) {
          return;
        }
      }

      // ノードを削除した際に、そのノードに紐づくリレーションシップの削除は省略

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

      //
      // リレーションシップの最新順インデックスRowの削除
      //
      List<Delete> indexDelete = new ArrayList<Delete>();

      // リレーションシップの最新順インデックスのRowKeyを作成する
      List<byte[]> indexRows = createNewOrderIndexRows(startNodeId, type, endNodeId, deleteTimestamp);

      // Deleteオブジェクトの生成
      for (byte[] row : indexRows) {
        Delete delete = new Delete(row, deleteTimestamp); // Timestampには削除時間を指定
        indexDelete.add(delete);
      }

      // バッチ処理でDelete
      table.batch(indexDelete);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // ノードのプロパティの取得
  @Override
  public Map<String, String> getNodeProperties(String nodeId) throws IOException {
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      // ノードのRowKeyを作成する
      byte[] row = createNodeRow(nodeId);

      // Getオブジェクトの生成
      Get get = new Get(row);
      get.addColumn(COLUMN_FAMILY, PROPERTY_COLUMN);

      // 結果を取得
      Result result = table.get(get);

      if (result.isEmpty()) {
        // ノードが存在しない場合
        return null;
      }

      // プロパティ
      byte[] propertiesBytes = result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN);
      Map<String, String> properties = deserializeProperty(propertiesBytes); // デシリアライズ

      return properties;
    } finally {
      table.close();
    }
  }

  // リレーションシップのプロパティの取得
  @Override
  public Map<String, String> getRelationshipProperties(String startNodeId, String type, String endNodeId) throws IOException {
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      // リレーションシップのRowKeyを作成する
      byte[] row = createRelationshipRow(startNodeId, type, endNodeId);

      // Getオブジェクトの生成
      Get get = new Get(row);
      get.addColumn(COLUMN_FAMILY, PROPERTY_COLUMN);

      // 結果を取得
      Result result = table.get(get);

      if (result.isEmpty()) {
        // リレーションシップが存在しない場合
        return null;
      }

      // プロパティ
      byte[] propertiesBytes = result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN);
      Map<String, String> properties = deserializeProperty(propertiesBytes); // デシリアライズ

      return properties;
    } finally {
      table.close();
    }
  }

  // 隣接リレーションシップの取得(最新順)
  @Override public List<Relationship> select(String nodeId, String type, Direction direction, int length) throws IOException {
    // startRow
    byte[] startRow = createNowOrderIndexScanStartRow(nodeId, type, direction);

    // startRowをインクリメントしたものをstopRowとする
    byte[] stopRow = createNowOrderIndexScanStartRow(nodeId, type, direction);
    incrementBytes(stopRow); // バイト配列をインクリメントする

    // Scanオブジェクトを生成する
    Scan scan = new Scan(startRow, stopRow);
    scan.addFamily(COLUMN_FAMILY);

    HTableInterface table = hTablePool.getTable(TABLE);
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);

      // Scanして結果を取得する
      List<Relationship> ret = new ArrayList<Relationship>();
      for (Result result : scanner) {
        Relationship relationship = new Relationship();
        ret.add(relationship);

        relationship.setType(type);
        relationship.setProperties(deserializeProperty(result.getValue(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */))); // プロパティを取得してセット

        switch (direction) {
        case INCOMING:
          relationship.setStartNodeId(extractNodeIdFromNewOrderIndexRow(result.getRow())); // RowKeyからノードIDを取得
          relationship.setEndNodeId(nodeId);
          break;
        case OUTGOING:
          relationship.setStartNodeId(nodeId);
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

      return ret;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      table.close();
    }
  }

  // ノードのプロパティの追加・更新・削除
  @Override
  public void updateNodeProperties(String nodeId, Map<String, String> putProperties, Set<String> deletePropertyNames) throws IOException {
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
          return;
        }

        // 現在の更新時間
        byte[] lastUpdateTimestampBytes = result.getValue(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN);
        long lastUpdateTimestamp = Bytes.toLong(lastUpdateTimestampBytes);

        // 現在のプロパティ
        Map<String, String> lastProperties = deserializeProperty(result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN));

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
        Put put = new Put(row, updateTimestamp);  // Timestampには更新時間を指定
        put.add(COLUMN_FAMILY, PROPERTY_COLUMN, propertiesBytes);
        put.add(COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, Bytes.toBytes(updateTimestamp));

        // checkAndPut。falseが返ってきたら、もう一度繰り返す
        boolean success = table.checkAndPut(row, COLUMN_FAMILY, UPDATE_TIMESTAMP_COLUMN, lastUpdateTimestampBytes, put);
        if (success) {
          return;
        }
      }
    } finally {
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
        Map<String, String> lastProperties = deserializeProperty(result.getValue(COLUMN_FAMILY, PROPERTY_COLUMN));

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

      //
      // リレーションシップの最新順インデックスRowの更新
      //
      List<Put> indexPut = new ArrayList<Put>();

      // リレーションシップの最新順インデックスのRowKeyを作成する
      List<byte[]> indexRows = createNewOrderIndexRows(startNodeId, type, endNodeId, createTimestamp);

      // Putオブジェクトの生成
      for (byte[] row : indexRows) {
        Put put = new Put(row, updateTimestamp); // Timestampには更新時間を指定
        put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY /* カラム名は空 */, propertiesBytes);
        indexPut.add(put);
      }

      // バッチ処理でPut
      table.batch(indexPut);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // ノードのRowKeyの作成
  protected byte[] createNodeRow(String nodeId) {
    byte[] nodeIdBytes = Bytes.toBytes(nodeId);
    ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + 4 + nodeIdBytes.length); // int型 + byte型 + int型 + nodeIdのバイト配列
    buffer.putInt(nodeId.hashCode()) // hash
          .put((byte) 0) // 0
          .putInt(nodeIdBytes.length) // nodeIdのバイト数
          .put(nodeIdBytes); // nodeIdのバイト配列
    return buffer.array();
  }

  // リレーションシップの最新順インデックスRowからノードIDを抽出する
  protected String extractNodeIdFromNewOrderIndexRow(byte[] row) {
    ByteBuffer buffer = ByteBuffer.wrap(row);

    byte[] bytes;

    buffer.getInt(); // hash
    buffer.get(); // 2

    // nodeId
    bytes = new byte[buffer.getInt()];
    buffer.get(bytes);

    buffer.get(); // direction

    // type
    bytes = new byte[buffer.getInt()];
    buffer.get(bytes);

    buffer.getLong(); // reverse timestamp

    // nodeId
    bytes = new byte[buffer.getInt()];
    buffer.get(bytes);

    return Bytes.toString(bytes);
  }

  // バイト配列をインクリメントする
  protected byte[] incrementBytes(byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      boolean increase = false;

      final int val = bytes[bytes.length - (i + 1)] & 0x0ff;
      int total = val + 1;
      if (total > 255) {
        increase = true;
        total = 0;
      }
      bytes[bytes.length - (i + 1)] = (byte) total;
      if (!increase) {
        return bytes;
      }
    }
    return bytes;
  }

  // リレーションシップの最新順インデックスのRowKeyの作成
  protected List<byte[]> createNewOrderIndexRows(String startNodeId, String type, String endNodeId, long createTimestamp) {
    byte[] startNodeIdBytes = Bytes.toBytes(startNodeId);
    byte[] typeBytes = Bytes.toBytes(type);
    byte[] endNodeIdBytes = Bytes.toBytes(endNodeId);
    long reverseTimestamp = Long.MAX_VALUE - createTimestamp;

    List<byte[]> rows = new ArrayList<byte[]>();

    // IMCOMING
    ByteBuffer incomingBuffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + endNodeIdBytes.length // int型 + endNodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 8 // long型
        + 4 + startNodeIdBytes.length // int型 + startNodeIdのバイト配列
    );
    incomingBuffer.putInt(endNodeId.hashCode()) // hash
                  .put((byte) 2) // 2
                  .putInt(endNodeIdBytes.length) // endNodeIdのバイト数
                  .put(endNodeIdBytes) // endNodeIdのバイト配列
                  .put(getDirectionByte(Direction.INCOMING)) // IMCOMING
                  .putInt(typeBytes.length) // typeのバイト数
                  .put(typeBytes) // typeのバイト配列
                  .putLong(reverseTimestamp) // Long.MAX_VALUE - createTimestamp
                  .putInt(startNodeIdBytes.length) // startNodeIdのバイト数
                  .put(startNodeIdBytes); // startNodeIdのバイト配列

    rows.add(incomingBuffer.array());

    // OUTGOING
    ByteBuffer outgoingBuffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + startNodeIdBytes.length // int型 + endNodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 8 // long型
        + 4 + endNodeIdBytes.length // int型 + startNodeIdのバイト配列
    );
    outgoingBuffer.putInt(startNodeId.hashCode()) // hash
                  .put((byte) 2) // 2
                  .putInt(startNodeIdBytes.length) // startNodeIdのバイト数
                  .put(startNodeIdBytes) // startNodeIdのバイト配列
                  .put(getDirectionByte(Direction.OUTGOING)) // OUTGOING
                  .putInt(typeBytes.length) // typeのバイト数
                  .put(typeBytes) // typeのバイト配列
                  .putLong(reverseTimestamp) // Long.MAX_VALUE - createTimestamp
                  .putInt(endNodeIdBytes.length) // endNodeIdのバイト数
                  .put(endNodeIdBytes); // endNodeIdのバイト配列

    rows.add(outgoingBuffer.array());

    return rows;
  }

  // リレーションシップの最新順インデックスRowをScanするためのstartRowを生成する。hash(nodeId)-2-nodeId-direction-type
  protected byte[] createNowOrderIndexScanStartRow(String nodeId, String type, Direction direction) {
    byte[] nodeIdBytes = Bytes.toBytes(nodeId);
    byte[] typeBytes = Bytes.toBytes(type);

    ByteBuffer buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + nodeIdBytes.length // int型 + nodeIdのバイト配列
        + 1 // byte型
        + 4 + typeBytes.length // int型 + typeのバイト配列
    );
    buffer.putInt(nodeId.hashCode()) // hash
          .put((byte) 2) // 2
          .putInt(nodeIdBytes.length) // nodeIdのバイト数
          .put(nodeIdBytes) // nodeIdのバイト配列
          .put(getDirectionByte(direction)) // direction
          .putInt(typeBytes.length) // typeのバイト数
          .put(typeBytes); // typeのバイト配列
    return buffer.array();
  }

  // リレーションシップのRowKeyの作成
  protected byte[] createRelationshipRow(String startNodeId, String type, String endNodeId) {
    byte[] startNodeIdBytes = Bytes.toBytes(startNodeId);
    byte[] typeBytes = Bytes.toBytes(type);
    byte[] endNodeIdBytes = Bytes.toBytes(endNodeId);

    ByteBuffer buffer = ByteBuffer.allocate(4 + 1 // int型 + byte型
        + 4 + startNodeIdBytes.length // int型 + startNodeIdのバイト配列
        + 4 + typeBytes.length // int型 + typeのバイト配列
        + 4 + endNodeIdBytes.length // int型 + endNodeIdのバイト配列
    );
    buffer.putInt(startNodeId.hashCode()) // hash
          .put((byte) 1) // 1
          .putInt(startNodeIdBytes.length) // startNodeIdのバイト数
          .put(startNodeIdBytes) // startNodeIdのバイト配列
          .putInt(typeBytes.length) // typeのバイト数
          .put(typeBytes) // typeのバイト配列
          .putInt(endNodeIdBytes.length) // endNodeIdのバイト数
          .put(endNodeIdBytes); // endNodeIdのバイト配列
    return buffer.array();
  }

  // バイト配列からプロパティにデシリアライズする
  protected Map<String, String> deserializeProperty(byte[] bytes) {
    if (bytes.length == 0) {
      return new HashMap<String, String>();
    }

    final ByteBuffer buffer = ByteBuffer.wrap(bytes);

    final int size = buffer.getInt();

    final Map<String, String> ret = new HashMap<String, String>();
    for (int i = 0; i < size; i++) {
      final byte[] keyBytes = new byte[buffer.getInt()];
      buffer.get(keyBytes);

      final byte[] value = new byte[buffer.getInt()];
      buffer.get(value);

      ret.put(Bytes.toString(keyBytes), Bytes.toString(value));
    }

    return ret;
  }

  // 方向のバイト表現を返す(INCOMINGは1,OUTGOINGは2)
  protected byte getDirectionByte(Direction direction) {
    switch (direction) {
    case INCOMING:
      return (byte) 1;
    case OUTGOING:
      return (byte) 2;
    default:
      throw new AssertionError();
    }
  }

  // プロパティをバイト配列にシリアライズする
  protected byte[] serializeProperty(Map<String, String> properties) {
    if (properties.isEmpty()) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    int size = 0;
    final List<byte[]> bytesList = new ArrayList<byte[]>();

    bytesList.add(Bytes.toBytes(properties.size()));
    size += 4;

    for (final Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getValue() != null) {
        final byte[] keyBytes = Bytes.toBytes(entry.getKey());
        bytesList.add(Bytes.toBytes(keyBytes.length));
        bytesList.add(keyBytes);
        size += 4 + keyBytes.length;

        final byte[] valueBytes = Bytes.toBytes(entry.getValue());
        bytesList.add(Bytes.toBytes(valueBytes.length));
        bytesList.add(valueBytes);
        size += 4 + valueBytes.length;
      }
    }

    final ByteBuffer buffer = ByteBuffer.allocate(size);
    for (final byte[] bytes : bytesList) {
      buffer.put(bytes);
    }

    return buffer.array();
  }
}
