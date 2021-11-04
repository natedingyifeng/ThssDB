package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
  public String name;
  public ColumnType type;
  public int primary;
  public boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  /**
   * 从一行 MetaData 字符串生成Column，该行字符串是Column的toString方法生成的。
   * @param line toString方法生成的字符串
   * @author XuYihao
   */
  public Column(String line){
    String[] param = line.split(",");
    this.name = param[0];
    this.type = ColumnType.valueOf(param[1]);
    this.primary = Integer.parseInt(param[2]);
    this.notNull = Boolean.parseBoolean(param[3]);
    this.maxLength = Integer.parseInt(param[4]);
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }

  public boolean isPrimary() {
    return this.primary==1;
  }

  public ColumnType getType() {
    return this.type;
  }

  public boolean getNotNull() {
    return this.notNull;
  }

  public int getMaxLength() {
    return this.maxLength;
  }

  public String getName() { return this.name; }

  public void setPrimaryKey() { this.primary = 1; }
}


