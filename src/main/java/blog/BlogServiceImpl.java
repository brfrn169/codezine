package blog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class BlogServiceImpl implements BlogService {
  // Table名
  private static final String TABLE_NAME = "blog";

  // ColumnFamily名
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");

  // シーケンスRowKey
  private static final byte[] SEQUENCE_ROW = new byte[]{0x00};

  private final HTablePool hTablePool;

  // コンストラクタ
  public BlogServiceImpl(Configuration conf) {
    hTablePool = new HTablePool(conf, Integer.MAX_VALUE);
  }

  // ブログ記事削除
  @Override
  public void deleteArticle(Article article) throws IOException {
    long deleteAt = System.currentTimeMillis(); // 削除時間

    // Deleteオブジェクトの作成
    byte[] row = createRow(article.getUserId(), article.getPostAt(), article.getArticleId());
    Delete delete = new Delete(row, deleteAt); // Timestampにデータを削除するときの時間を指定

    // セカンダリインデックスのDeleteオブジェクトの作成
    byte[] secondaryIndexRow = createSecondaryIndexRow(article.getUserId(), article.getCategoryId(), article.getPostAt(),
        article.getArticleId());
    Delete secondaryIndexDelete = new Delete(secondaryIndexRow, deleteAt); // Timestampにデータを削除するときの時間を指定

    // バッチ処理のためにリストの格納
    List<Row> deletes = new ArrayList<Row>();
    deletes.add(delete);
    deletes.add(secondaryIndexDelete);

    // プールからHTableInterfaceを取得
    HTableInterface table = hTablePool.getTable(TABLE_NAME);
    try {
      // バッチ処理でDelete
      table.batch(deletes);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // ブログ記事の取得(最新順)
  @Override
  public List<Article> getArticles(long userId, Article lastArticle, int length) throws IOException {
    // Scanオブジェクト
    Scan scan = new Scan();
    if (lastArticle == null) {
      scan.setStartRow(createStartRow(userId));
    } else {
      // ページング処理
      scan.setStartRow(createPagingStartRow(userId, lastArticle.getPostAt(), lastArticle.getArticleId()));
    }
    scan.setStopRow(createStopRow(userId));

    List<Article> ret = new ArrayList<Article>();

    // プールからHTableInterfaceを取得
    HTableInterface table = hTablePool.getTable(TABLE_NAME);
    ResultScanner scanner = null;
    try {
      // ResultScannerの取得
      scanner = table.getScanner(scan);

      for (Result result : scanner) {
        // データを取得
        byte[] value = result.getValue(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY);
        // データのデシリアライズ
        Article article = deserialize(value);
        ret.add(article);
        if (ret.size() >= length) {
          break;
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      table.close();
    }
    return ret;
  }

  // ブログ記事の取得(カテゴリ別)
  @Override
  public List<Article> getArticles(long userId, int categoryId, Article lastArticle, int length) throws IOException {
    // Scanオブジェクト
    Scan scan = new Scan();

    if (lastArticle == null) {
      scan.setStartRow(createSecondaryIndexStartRow(userId, categoryId));
    } else {
      // ページング処理
      scan.setStartRow(createSecondaryIndexPagingStartRow(userId, categoryId, lastArticle.getPostAt(), lastArticle.getArticleId()));
    }
    scan.setStopRow(createSecondaryIndexStopRow(userId, categoryId));

    List<Article> ret = new ArrayList<Article>();

    // プールからHTableInterfaceを取得
    HTableInterface table = hTablePool.getTable(TABLE_NAME);
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);

      for (Result result : scanner) {
        byte[] value = result.getValue(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY);
        Article article = deserialize(value);
        ret.add(article);
        if (ret.size() >= length) {
          break;
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      table.close();
    }
    return ret;
  }

  // ブログ記事投稿
  @Override
  public void postArticle(long userId, String title, String content, int categoryId) throws IOException {
    // ColumnFamily名とTable名。説明のためにここで定義してるが、本来はフィールドなどで定義するべき。
    final byte[] COLUMN_FAMILY = Bytes.toBytes("d");
    final String TABLE_NAME = "blog";

    long postAt = System.currentTimeMillis(); // 投稿日時
    long updateAt = postAt; // 更新日時
    String userName = getUserName(userId); // ユーザ名の取得
    String cagegoryName = getCategoryName(categoryId); // カテゴリ名の取得
    long articleId = createArticleId(); // 記事IDの生成

    // データのシリアライズ
    byte[] serializedData = serialize(articleId, userId, userName, title, content, categoryId, cagegoryName, postAt, updateAt);

    // Putオブジェクトの作成
    byte[] row = createRow(userId, postAt, articleId);
    Put put = new Put(row, postAt); // Timestampにデータを追加するときの時間を指定
    put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY, serializedData);

    // セカンダリインデックスのPutオブジェクトの作成
    byte[] secondaryIndexRow = createSecondaryIndexRow(userId, categoryId, postAt, articleId);
    Put secondaryIndexPut = new Put(secondaryIndexRow, postAt); // Timestampにデータを追加するときの時間を指定
    secondaryIndexPut.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY, serializedData);

    // バッチ処理のためにリストの格納
    List<Row> puts = new ArrayList<Row>();
    puts.add(put);
    puts.add(secondaryIndexPut);

    // プールからHTableInterfaceを取得
    HTableInterface table = hTablePool.getTable(TABLE_NAME);
    try {
      // バッチ処理でPut
      table.batch(puts);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // 各データをbyte[]にシリアライズする
  public byte[] serialize(long articleId, long userId, String userName, String title, String content, int categoryId, String categoryName,
      long postAt, long updateAt) {
    Article article = new Article();
    article.setArticleId(articleId);
    article.setUserId(userId);
    article.setUserName(userName);
    article.setTitle(title);
    article.setContent(content);
    article.setCategoryId(categoryId);
    article.setCategoryName(categoryName);
    article.setPostAt(postAt);
    article.setUpdateAt(updateAt);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream);
      out.writeObject(article);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return byteArrayOutputStream.toByteArray();
  }

  // ブログ記事更新
  @Override
  public void updateArticle(Article article, String newTitle, String newContent) throws IOException {
    long updateAt = System.currentTimeMillis(); // 更新時間

    // 更新後のデータをシリアライズ
    byte[] serializedData = serialize(article.getArticleId(), article.getUserId(), article.getUserName(), newTitle, newContent,
        article.getCategoryId(), article.getCategoryName(), article.getPostAt(), updateAt);

    // Putオブジェクトの作成
    byte[] row = createRow(article.getUserId(), article.getPostAt(), article.getArticleId());
    Put put = new Put(row, updateAt); // Timestampにデータを更新するときの時間を指定
    put.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY, serializedData);

    // セカンダリインデックスのPutオブジェクトの作成
    byte[] secondaryIndexRow = createSecondaryIndexRow(article.getUserId(), article.getCategoryId(), article.getPostAt(),
        article.getArticleId());
    Put secondaryIndexPut = new Put(secondaryIndexRow, updateAt); // Timestampにデータを更新するときの時間を指定
    secondaryIndexPut.add(COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY, serializedData);

    // バッチ処理のためにリストの格納
    List<Row> puts = new ArrayList<Row>();
    puts.add(put);
    puts.add(secondaryIndexPut);

    // プールからHTableInterfaceを取得
    HTableInterface table = hTablePool.getTable(TABLE_NAME);
    try {
      // バッチ処理でPut
      table.batch(puts);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      table.close();
    }
  }

  // 記事IDの生成(HBaseのincrementColumnValueを利用)
  private long createArticleId() throws IOException {
    // プールからHTableInterfaceを取得
    HTableInterface table = hTablePool.getTable(TABLE_NAME);
    try {
      return table.incrementColumnValue(SEQUENCE_ROW, COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY, 1L);
    } finally {
      table.close();
    }
  }

  // ページング用startRowの作成
  private byte[] createPagingStartRow(long userId, long lastPostAt, long lastArticleId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1 + 8 + 8); // int型 + long型 + byte型 + long型 + long型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) 0); // 0
    buffer.putLong(Long.MAX_VALUE - lastPostAt); // Long.MAX_VALUE - lastPostAt
    buffer.putLong(lastArticleId + 1); // articleId + 1。最後の記事は含めないので+1する
    return buffer.array();
  }

  // RowKeyの作成。hash(userId)-userId-0-(Long.MAX_VALUE - postAt)-articleId
  private byte[] createRow(long userId, long postAt, long articleId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1 + 8 + 8); // int型 + long型 + byte型 + long型 + long型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) 0); // 0
    buffer.putLong(Long.MAX_VALUE - postAt); // Long.MAX_VALUE - postAt
    buffer.putLong(articleId); // articleId
    return buffer.array();
  }

  // セカンダリインデックスのページング用startRowの作成
  private byte[] createSecondaryIndexPagingStartRow(long userId, int categoryId, long lastPostAt, long lastArticleId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1 + 4 + 8 + 8); // int型 + long型 + byte型 + int型 + long型 + long型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) 1); // 1
    buffer.putInt(categoryId); // categoryId
    buffer.putLong(Long.MAX_VALUE - lastPostAt); // Long.MAX_VALUE - lastPostAt
    buffer.putLong(lastArticleId + 1); // articleId + 1。最後の記事は含めないので+1する
    return buffer.array();
  }

  // セカンダリインデックスのRowKeyの作成。hash(userId)-userId-1-categoryId-(Long.MAX_VALUE - postAt)-articleId
  private byte[] createSecondaryIndexRow(long userId, int categoryId, long postAt, long articleId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1 + 4 + 8 + 8); // int型 + long型 + byte型 + int型 + long型 + long型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) 1); // 1
    buffer.putInt(categoryId); // categoryId
    buffer.putLong(Long.MAX_VALUE - postAt); // Long.MAX_VALUE - postAt
    buffer.putLong(articleId); // articleId
    return buffer.array();
  }

  // セカンダリインデックスのstartRowの作成
  private byte[] createSecondaryIndexStartRow(long userId, int categoryId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1 + 4); // int型 + long型 + byte型 + int型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) 1); // 1
    buffer.putInt(categoryId); // categoryId
    return buffer.array();
  }

  // セカンダリインデックスのstopRowの作成
  private byte[] createSecondaryIndexStopRow(long userId, int categoryId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1 + 4); // int型 + long型 + byte型 + int型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) 1); // 1
    buffer.putInt(categoryId + 1); // categoryId + 1。パーシャルスキャン
    return buffer.array();
  }

  // startRowの作成
  private byte[] createStartRow(long userId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1); // int型 + long型 + byte型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) 0); // 0
    return buffer.array();
  }

  // stopRowの作成
  private byte[] createStopRow(long userId) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 1); // int型 + long型 + byte型
    buffer.putInt(hash(userId)); // hash(userId)
    buffer.putLong(userId); // userId
    buffer.put((byte) (0 + 1)); // 0 + 1。バーシャルスキャン
    return buffer.array();
  }

  // byte[]からArticleにデシリアライズする
  private Article deserialize(byte[] bytes) {
    try {
      ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
      return (Article) objectInputStream.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // カテゴリ名の取得(ダミー。受け取ったcategoryIdをStringに変換してそのまま返している)
  private String getCategoryName(int categoryId) {
    return Integer.toString(categoryId);
  }

  // ユーザ名の取得(ダミー。受け取ったuserIdをStringに変換してそのまま返している)
  private String getUserName(long userId) {
    return Long.toString(userId);
  }

  // ハッシュ関数
  private int hash(long value) {
    return (int) (value ^ value >>> 32);
  }
}
