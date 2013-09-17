package blog;

import java.io.Serializable;

public class Article implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  // 記事ID
  private long articleId;

  // カテゴリID
  private int categoryId;

  // カテゴリ名
  private String categoryName;

  // 本文
  private String content;

  // 投稿日時(タイムスタンプ)
  private long postAt;

  // タイトル
  private String title;

  // 更新日時(タイムスタンプ)
  private long updateAt;

  // ユーザID
  private long userId;

  // ユーザ名
  private String userName;

  public long getArticleId() {
    return articleId;
  }

  public int getCategoryId() {
    return categoryId;
  }

  public String getCategoryName() {
    return categoryName;
  }

  public String getContent() {
    return content;
  }

  public long getPostAt() {
    return postAt;
  }

  public String getTitle() {
    return title;
  }

  public long getUpdateAt() {
    return updateAt;
  }

  public long getUserId() {
    return userId;
  }

  public String getUserName() {
    return userName;
  }

  public void setArticleId(long articleId) {
    this.articleId = articleId;
  }

  public void setCategoryId(int categoryId) {
    this.categoryId = categoryId;
  }

  public void setCategoryName(String categoryName) {
    this.categoryName = categoryName;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public void setPostAt(long postAt) {
    this.postAt = postAt;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setUpdateAt(long updateAt) {
    this.updateAt = updateAt;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }
}
