package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class QueryTable implements Iterator<Row> {

  private Iterator<Row> iterator;
  private HashMap<String, Pair<Integer, Column>> columns = new HashMap<>();

  public QueryTable(Table table) {
    this.iterator = table.iterator();
    int i = 0;
    for(Column column : table.columns){
      columns.put(column.name, new Pair<>(i, column));
      i++;
    }
  }

  public QueryTable(Table table1, Table table2, ArrayList<Row> rows) {
    this.iterator = rows.iterator();
    int i = 0;
    for(Column column : table1.columns){
      columns.put(column.name, new Pair<>(i, column));
      i++;
    }
    for(Column column : table2.columns){
      if (!columns.containsKey(column.name)){
        columns.put(column.name, new Pair<>(i, column));
      }
      i++;
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Row next() {
    return iterator.next();
  }

  public Pair<Integer, Column> getColumn(String name){
    return columns.get(name);
  }
}