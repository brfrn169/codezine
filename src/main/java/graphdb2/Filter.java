package graphdb2;

public class Filter {
  // フィルタリングするプロパティ名
  private String propertyName;

  // オペレータ
  private Operator operator;

  // フィルタリングする基準となる値
  private String value;

  public Operator getOperator() {
    return operator;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public String getValue() {
    return value;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
