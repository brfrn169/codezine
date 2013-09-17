package access;

import java.util.Calendar;

public class Access {
  // アクセス数
  private long count;

  // ドメイン
  private String domain;

  // パス
  private String path;

  // 時間
  private Calendar time;

  public long getCount() {
    return count;
  }

  public String getDomain() {
    return domain;
  }

  public String getPath() {
    return path;
  }

  public Calendar getTime() {
    return time;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setTime(Calendar time) {
    this.time = time;
  }
}
