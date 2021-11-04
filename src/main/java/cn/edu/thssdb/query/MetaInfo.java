package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class MetaInfo {

  private String tableName;
  private List<Column> columns;

  public MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  String getTableName(){
    return tableName;
  }

  public int columnFind(String name) {
    int index=0;
    for(Column item:columns)
    {
      if (name.toUpperCase().equals(item.getName().toUpperCase()))
      {
        return index;
      }
      else index = index + 1;
    }
    return -1;
  }

  public int getSize(){
    return columns.size();
  }
}