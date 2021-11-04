package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.tableException.TableAttrNotExistException;
import cn.edu.thssdb.exception.tableException.TableNotExistException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  private List<MetaInfo> metaInfoInfos;
  private List<Integer> index;
  private List<Cell> attrs;

  public QueryResult(QueryTable[] queryTables) {
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
    this.metaInfoInfos = new ArrayList<>();
  }

  public QueryResult(){
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
    this.metaInfoInfos = new ArrayList<>();
  }

  public static Row combineRow(LinkedList<Row> rows) {
    ArrayList<Entry> entries = new ArrayList<>();
    for(Row row:rows){
      entries.addAll(row.getEntries());
    }
    Row row_combined = new Row();
    row_combined.appendEntries(entries);
    return row_combined;
  }

  public static Row combineRow(Row row1, Row row2) {
    ArrayList<Entry> entries = new ArrayList<>();
    entries.addAll(row1.getEntries());
    entries.addAll(row2.getEntries());
    Row resultRow = new Row();
    resultRow.appendEntries(entries);
    return resultRow;
  }

  public Row generateQueryRecord(Row row) {
    Row row_new = new Row();
    ArrayList<Entry> entries = new ArrayList<>();
    for(int i: index){
      entries.add(row.getEntries().get(i));
    }
    row_new.appendEntries(entries);
    return row_new;
  }

  public void addMetaInfo(MetaInfo metaInfo){
    this.metaInfoInfos.add(metaInfo);
  }

  public void addIndex(String tableName,String attrName){
    int position = 0;
    boolean flag = false;
    for(MetaInfo m: this.metaInfoInfos){
      if(m.getTableName().equals(tableName)){
        int findPosition = m.columnFind(attrName);
        if(findPosition == m.getSize()) {
          throw new TableAttrNotExistException(tableName,attrName);
        }
        else {
          position += findPosition;
          this.index.add(position);
          flag = true;
        }
        break;
      }
      else position += m.getSize();
    }
    if(!flag) throw new TableNotExistException();
  }
}
