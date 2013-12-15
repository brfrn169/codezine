package graphdb3;

import graphdb.Direction;

import java.io.Serializable;

public class Cursor implements Serializable {

  // カーソルのタイプ
  public enum CursorType {
    NEW_ORDER_INDEX, // 最新順
    SECONDARY_INDEX // セカンダリインデックスを使用
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  // カーソルのタイプ
  private CursorType cursorType;

  // 方向
  private Direction direction;

  // ノードID
  private String nodeId;

  // ScanのstartRow
  private byte[] startRow;

  // ScanのstopRow
  private byte[] stopRow;

  // リレーションシップのタイプ
  private String type;

  public CursorType getCursorType() {
    return cursorType;
  }

  public Direction getDirection() {
    return direction;
  }

  public String getNodeId() {
    return nodeId;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public String getType() {
    return type;
  }

  public void setCursorType(final CursorType cursorType) {
    this.cursorType = cursorType;
  }

  public void setDirection(final Direction direction) {
    this.direction = direction;
  }

  public void setNodeId(final String nodeId) {
    this.nodeId = nodeId;
  }

  public void setStartRow(final byte[] startRow) {
    this.startRow = startRow;
  }

  public void setStopRow(final byte[] stopRow) {
    this.stopRow = stopRow;
  }

  public void setType(final String type) {
    this.type = type;
  }
}
