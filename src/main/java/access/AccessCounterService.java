package access;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

public interface AccessCounterService {
  // アクセスをカウントする
  void count(String domain, String path, int amount) throws IOException;

  // デイリー(毎日)のアクセスを取得する
  List<Access> getDailyCount(String domain, String path, Calendar startDay, Calendar endDay) throws IOException;

  // アワリー(毎時)のアクセスを取得する
  List<Access> getHourlyCount(String domain, String path, Calendar startHour, Calendar endHour) throws IOException;

  // トータルのアクセスを取得する
  List<Access> getTotalCount(String domain, String path) throws IOException;
}
