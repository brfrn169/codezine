package blog;

import java.io.IOException;
import java.util.List;

public interface BlogService {
  // ブログ記事削除
  void deleteArticle(Article article) throws IOException;

  // ブログ記事の取得(最新順)
  List<Article> getArticles(long userId, Article lastArticle, int length) throws IOException;

  // ブログ記事の取得(カテゴリ別)
  List<Article> getArticles(long userId, int categoryId, Article lastArticle, int length) throws IOException;

  // ブログ記事投稿
  void postArticle(long userId, String title, String content, int categoryId) throws IOException;

  // ブログ記事更新
  void updateArticle(Article article, String newTitle, String newContent) throws IOException;
}
