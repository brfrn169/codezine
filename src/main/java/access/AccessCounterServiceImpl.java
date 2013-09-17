package access;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class AccessCounterServiceImpl implements AccessCounterService {
  // Table名
  private static final String TABLE = "access";

  // ColumnFamily名
  private static final byte[] DAILY_COLUMN_FAMILY = Bytes.toBytes("d"); // デイリー
  private static final byte[] HOURLY_COLUMN_FAMILY = Bytes.toBytes("h"); // アワリー
  private static final byte[] TOTAL_COLUMN_FAMILY = Bytes.toBytes("t"); // トータル

  private final HTablePool hTablePool;

  // コンストラクタ
  public AccessCounterServiceImpl(Configuration conf) {
    hTablePool = new HTablePool(conf, Integer.MAX_VALUE);
  }

  // アクセスをカウントする
  @Override
  public void count(String domain, String path, int amount) throws IOException {
    // RowKeyの作成
    String reversedDomain = reverseDomain(domain); // reverse domainの作成
    byte[] row = createRow(reversedDomain, path);

    // Incrementオブジェクトの生成
    Increment increment = new Increment(row);
    increment.addColumn(HOURLY_COLUMN_FAMILY, createHourlyQualifier(), amount); // アワリー
    increment.addColumn(DAILY_COLUMN_FAMILY, createDailyQualifier(), amount); // デイリー
    increment.addColumn(TOTAL_COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY, amount); // トータル

    // インクリメント
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      table.increment(increment);
    } finally {
      table.close();
    }
  }

  // デイリー(毎日)のアクセスを取得する
  @Override
  public List<Access> getDailyCount(String domain, String path, Calendar startDay, Calendar endDay) throws IOException {
    // Scanの作成
    Scan scan;
    String reversedDomain = reverseDomain(domain);
    if (path != null) {
      byte[] row = createRow(reversedDomain, path);
      scan = new Scan(row, row);
    } else {
      byte[] prefix = Bytes.toBytes(reversedDomain);
      scan = new Scan(prefix);
      scan.setFilter(new PrefixFilter(prefix));
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    // startDayとendDayから取得したいColumnを指定
    Calendar cal = (Calendar) startDay.clone();
    while (cal.before(endDay)) {
      scan.addColumn(DAILY_COLUMN_FAMILY, Bytes.toBytes(sdf.format(cal.getTime())));
      cal.add(Calendar.DAY_OF_MONTH, 1);
    }

    // 結果の取得
    List<Access> ret = new ArrayList<Access>();
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      ResultScanner scanner = table.getScanner(scan);

      for (Result result : scanner) {
        String[] domainAndPath = extractDomainAndPath(result.getRow());

        for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(DAILY_COLUMN_FAMILY).entrySet()) {
          byte[] qualifier = entry.getKey();
          byte[] value = entry.getValue();

          // 時間
          Calendar time = Calendar.getInstance();
          time.setTime(sdf.parse(Bytes.toString(qualifier)));

          // Accessオブジェクトの作成
          Access access = new Access();
          access.setTime(time);
          access.setDomain(reverseDomain(domainAndPath[0]));
          access.setPath(domainAndPath[1]);
          access.setCount(Bytes.toLong(value));

          ret.add(access);
        }
      }
    } catch (ParseException e) {
      // 省略
    } finally {
      table.close();
    }

    return ret;
  }

  // アワリー(毎時)のアクセスを取得する
  @Override
  public List<Access> getHourlyCount(String domain, String path, Calendar startHour, Calendar endHour) throws IOException {
    // Scanの作成
    Scan scan;
    String reversedDomain = reverseDomain(domain);
    if (path != null) {
      byte[] row = createRow(reversedDomain, path);
      scan = new Scan(row, row);
    } else {
      byte[] prefix = Bytes.toBytes(reversedDomain);
      scan = new Scan(prefix);
      scan.setFilter(new PrefixFilter(prefix));
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");

    // startHourとendHourから取得したいColumnを指定
    Calendar cal = (Calendar) startHour.clone();
    while (cal.before(endHour)) {
      scan.addColumn(HOURLY_COLUMN_FAMILY, Bytes.toBytes(sdf.format(cal.getTime())));
      cal.add(Calendar.HOUR_OF_DAY, 1);
    }

    // 結果の取得
    List<Access> ret = new ArrayList<Access>();
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      ResultScanner scanner = table.getScanner(scan);

      for (Result result : scanner) {
        String[] domainAndPath = extractDomainAndPath(result.getRow());

        for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(HOURLY_COLUMN_FAMILY).entrySet()) {
          byte[] qualifier = entry.getKey();
          byte[] value = entry.getValue();

          // 時間
          Calendar time = Calendar.getInstance();
          time.setTime(sdf.parse(Bytes.toString(qualifier)));

          // Accessオブジェクトの作成
          Access access = new Access();
          access.setTime(time);
          access.setDomain(reverseDomain(domainAndPath[0]));
          access.setPath(domainAndPath[1]);
          access.setCount(Bytes.toLong(value));

          ret.add(access);
        }
      }
    } catch (ParseException e) {
      // 省略
    } finally {
      table.close();
    }

    return ret;
  }

  // トータルのアクセスを取得する
  @Override
  public List<Access> getTotalCount(String domain, String path) throws IOException {
    // Scanの作成
    Scan scan;
    String reversedDomain = reverseDomain(domain);
    if (path != null) {
      byte[] row = createRow(reversedDomain, path);
      scan = new Scan(row, row);
    } else {
      byte[] prefix = Bytes.toBytes(reversedDomain);
      scan = new Scan(prefix);
      scan.setFilter(new PrefixFilter(prefix));
    }
    scan.addFamily(TOTAL_COLUMN_FAMILY); // ColumnFamilyを限定する

    // 結果の取得
    List<Access> ret = new ArrayList<Access>();
    HTableInterface table = hTablePool.getTable(TABLE);
    try {
      ResultScanner scanner = table.getScanner(scan);

      for (Result result : scanner) {
        String[] domainAndPath = extractDomainAndPath(result.getRow());

        // Accessオブジェクトの作成
        Access access = new Access();
        access.setDomain(reverseDomain(domainAndPath[0]));
        access.setPath(domainAndPath[1]);
        access.setCount(Bytes.toLong(result.getValue(TOTAL_COLUMN_FAMILY, HConstants.EMPTY_BYTE_ARRAY)));

        ret.add(access);
      }
    } finally {
      table.close();
    }

    return ret;
  }

  // 現在時刻から、デイリーのColumn名を作成する
  private byte[] createDailyQualifier() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    return Bytes.toBytes(sdf.format(new Date()));
  }

  // 現在時刻から、アワリーのColumn名を作成する
  private byte[] createHourlyQualifier() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
    return Bytes.toBytes(sdf.format(new Date()));
  }

  // RowKeyを作成する。reversedDomainとpathをタブ区切りで連結
  private byte[] createRow(String reversedDomain, String path) {
    return Bytes.toBytes(reversedDomain + "\t" + path);
  }

  // RowKeyからドメインとパスを抽出する
  private String[] extractDomainAndPath(byte[] row) {
    return Bytes.toString(row).split("\t");
  }

  // reverse domainを作成する
  private String reverseDomain(String domain) {
    String[] split = domain.split("\\.", 2);
    if (split.length == 1) {
      return domain;
    }
    return reverseDomain(split[1]) + "." + split[0];
  }
}
