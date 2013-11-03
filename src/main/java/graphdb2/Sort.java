package graphdb2;

public class Sort {
  // ソートするプロパティ名
  private String propertyName;

  // 順序(昇順、降順)
  private Order order;


  public Order getOrder() {
    return order;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public void setOrder(Order order) {
    this.order = order;
  }

  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }
}
