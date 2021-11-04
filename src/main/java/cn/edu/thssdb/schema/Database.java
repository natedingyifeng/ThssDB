package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.tableException.TableAlreadyExistException;
import cn.edu.thssdb.exception.tableException.TableNotExistException;
import cn.edu.thssdb.query.base.ConditionItem;
import cn.edu.thssdb.query.base.FromItem;
import cn.edu.thssdb.transaction.TransactionManager;
import cn.edu.thssdb.utils.ComplexDataOperation;
import cn.edu.thssdb.utils.Global;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {
  public String name;
  public HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;
  public HashMap<String, Table> droppedTables;
  public TransactionManager transactionManager;
  public ComplexDataOperation compare = new ComplexDataOperation();

  /**
   * Database元数据格式（每行）
   * 表名称
   */
  private MetaData metaData;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.metaData = new MetaData(Global.MANAGER_METADATA_DIR + name + "/", name + ".meta");
    this.droppedTables = new HashMap<>();
    this.transactionManager = new TransactionManager(this);
    recover();
  }

  public Database(String name,boolean isNeedRecover) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.metaData = new MetaData(Global.MANAGER_METADATA_DIR + name + "/", name + ".meta");
    this.droppedTables = new HashMap<>();
    if(isNeedRecover)
    {
      recover();
    }
    this.transactionManager = new TransactionManager(this, isNeedRecover);
  }

  public void persist() {
    // DONE
    metaData.writeMetaData(tables.keySet());
    for(Table table : tables.values()){
      table.persist();
    }
    for(Table droppedTable : droppedTables.values()){
      droppedTable.destroy();
    }
  }

  /**
   * 新建表
   * @param tableName 表名称
   * @param columns 列定义
   */
  public void create(String tableName, Column[] columns) {
    // DONE
    Table table = new Table(this.name, tableName, columns);
    tables.put(tableName, table);
  }

  public void create(String tableName, ArrayList<Column> columns) {
    if (!tables.containsKey(tableName)) {
      Table table = new Table(this.name, tableName, columns);
//      tables.put(name, table);
      tables.put(tableName, table);
      metaData.writeMetaData(tables.keySet());
    } else {
      throw new TableAlreadyExistException();
    }
  }

  /**
   * 从MetaData中恢复Database数据
   */
  private void recover() {
    // DONE
    List<String[]> tablesData = metaData.readMetaData();
    if(tablesData.isEmpty() || tablesData.get(0)[0] == "") return;
    for(String[] tableInfo : tablesData){
      Table table = new Table(this.name, tableInfo[0]);
      tables.put(tableInfo[0], table);
    }
  }

  public void quit() {
    // DONE
    persist();
  }

  /**
   * 删除数据库，以及删除相应文件
   */
  public void destroy(){
    for(Table table : tables.values()){
      table.destroy();
    }
    File metafile = new File(metaData.getFilepath());
    if(metafile.exists())
      metafile.delete();
    File metadir = new File(metaData.getFileDir());
    if(metadir.exists())
      metadir.delete();
    transactionManager.destroy();
  }

  public boolean containTable(String tableName){
    return tables.containsKey(tableName);
  }

  public Table getTable(String tableName){
    return tables.get(tableName);
  }

  public String getDBName(){
    return this.name;
  }

  public void drop(String tableName) {
    droppedTables.put(tableName, tables.remove(tableName));
    persist();
  }

  public String show_info(String name) {
    name = name.toUpperCase();
    if (tables.containsKey(name)) {
      Table table = tables.get(name);
      String str = "";
      int length = table.columns.size();
      for (int i = 0; i < length; ++i) {
        Column cul = table.columns.get(i);
        str = str + cul.name + ' ' + cul.type + "\n";
      }
      return str;
    } else {
      throw new TableNotExistException();
    }
  }

  public Object printTable(String name) {
    String str = "";
    try {
      Table table = tables.get(name);
      int length = table.columns.size();
      for (int i = 0; i < length; ++i) {
        Column cul = table.columns.get(i);
        str = str + cul.name + ' ';
      }
      System.out.println(str);
      str = str + '\n';
      Iterator<Row> rowIterator = table.iterator();
      while (rowIterator.hasNext())
      {
        str = str + rowIterator.next().getEntries().toString() + '\n';
//        System.out.println(rowIterator.next().getEntries().toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return str;
  }
}
