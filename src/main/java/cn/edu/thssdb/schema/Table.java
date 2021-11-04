package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  public ReentrantReadWriteLock lock;
  public Lock readLock;
  public Lock writeLock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;
  public String databaseFolderPath;
  private String metaDir;
  private String metaFilename;
  private String dataFilePath;

  /**
   * Table元数据格式
   * 第一行 databaseName 数据库名称
   * 第二行 tableName    表名称
   * 剩余行 column[]     列定义
   */
  private MetaData metaData;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName=databaseName;
    this.tableName=tableName;
    this.columns=new ArrayList<>();
    for(int i=0;i<columns.length;i++)
    {
      if(columns[i].isPrimary()){
        this.primaryIndex=i;
      }
      this.columns.add(columns[i]);
    }
    this.index=new BPlusTree<>();
    this.lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.databaseFolderPath =Global.TABLE_DIR + "/" + databaseName + "/tableData/";
    this.dataFilePath = this.databaseFolderPath + this.tableName + ".data";

    this.metaDir = Global.MANAGER_METADATA_DIR + databaseName + "/tableMeta/";
    this.metaFilename = this.tableName + ".meta";
    this.metaData = new MetaData(this.metaDir, this.metaFilename);
    this.metaData.setSplitString(","); // column的toString方法是用逗号分隔的
    recover();
  }


  public Table(String databaseName, String tableName, ArrayList<Column> columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = columns;
    this.databaseFolderPath =Global.TABLE_DIR + "/" + databaseName + "/";
    this.index = new BPlusTree<>();

    for (int i = 0; i < this.columns.size(); i++) {
      Column col = this.columns.get(i);
      if(col.isPrimary()){
        this.primaryIndex = i;
      }
    }
    lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    this.databaseFolderPath = Global.TABLE_DIR + "/" + databaseName + "/tableData/";
    this.dataFilePath = this.databaseFolderPath + this.tableName + ".data";
    this.metaDir = Global.MANAGER_METADATA_DIR + databaseName + "/tableMeta/";
    this.metaFilename = this.tableName + ".meta";
    this.metaData = new MetaData(this.metaDir, this.metaFilename);
    this.metaData.setSplitString(","); // column的toString方法是用逗号分隔的
    recover();
  }

  /**
   * 表已存在时的构造方法，从元数据中读取列定义，从持久化文件中读取表数据。
   * @param databaseName
   * @param tableName
   * @author XuYihao
   */
  public Table(String databaseName, String tableName){
    this.databaseName=databaseName;
    this.tableName=tableName;
    this.databaseFolderPath =Global.TABLE_DIR + "/" + databaseName + "/tableData/";
    this.columns=new ArrayList<>();

    this.metaData = new MetaData(Global.MANAGER_METADATA_DIR + databaseName + "/tableMeta/", tableName + ".meta");
    this.metaData.setSplitString(","); // column的toString方法是用逗号分隔的
    readMetaData();

    for(int i=0;i<columns.size();i++)
    {
      if(columns.get(i).isPrimary()){
        this.primaryIndex=i;
      }
    }

    this.index=new BPlusTree<>();
    this.lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    recover();
  }

  /**
   * 写入元数据
   * @author XuYihao
   */
  private void writeMetaData(){
    List<String> meta = new ArrayList<>();
    meta.add(this.databaseName);
    meta.add(this.tableName);
    for(Column column : columns){
      meta.add(column.toString());
    }
    this.metaData.writeMetaData(meta);
  }

  /**
   * 读取元数据
   * @author XuYihao
   */
  private void readMetaData(){
    List<String> meta = metaData.readMetaDataLine();
    this.databaseName = meta.get(0);
    this.tableName = meta.get(1);
    for(int i = 2; i < meta.size(); i++){
      columns.add(new Column(meta.get(i)));
    }
  }


  public void persist() {
    serialize();
    writeMetaData();
  }

  public void recover() {
    ArrayList<Row> rows = deserialize();
    this.index=new BPlusTree<>();
    for (Row row:rows) {
      this.index.put(row.getEntries().get(primaryIndex), row);
    }
  }

  //合法性检查
  public void validityCheckOfRow(Row row) {
    ArrayList<Entry> entries = row.getEntries();
    if(entries.size() != this.columns.size()){
      throw new IllegalArgumentException("The new data size does not match the table");
    }
    for(int i=0;i<entries.size();i++)
    {
      typeconvert(this.columns.get(i).getType().name(), entries.get(i));
      String str=entries.get(i).getValueType();
      if(this.columns.get(i).getNotNull()==true && entries.get(i)!=null || this.columns.get(i).getNotNull()==false) {
        switch (str) {
          case "java.lang.Integer":
            if (this.columns.get(i).getType().name() != "INT") {
              throw new IllegalArgumentException("The new data type does not match the table");
            }
            break;
          case "java.lang.Long":
            if (this.columns.get(i).getType().name() != "LONG") {
              throw new IllegalArgumentException("The new data type does not match the table");
            }
            break;
          case "java.lang.Double":
            if (this.columns.get(i).getType().name() != "DOUBLE") {
              throw new IllegalArgumentException("The new data type does not match the table");
            }
            break;
          case "java.lang.Float":
            if (this.columns.get(i).getType().name() != "FLOAT") {
              throw new IllegalArgumentException("The new data type does not match the table");
            }
            break;
          case "java.lang.String":
            if (this.columns.get(i).getType().name() != "STRING") {
              System.out.print(this.columns.get(i).getType().name());
              System.out.print(str);
              System.out.print(entries.get(i));
              throw new IllegalArgumentException("The new data type does not match the table");
            }
            break;
        }
      }

      if(str=="java.lang.String" && entries.get(i).toString().length()>this.columns.get(i).getMaxLength())
      {
        throw new IllegalArgumentException("The new data's length of string does not match the table");
      }
      if(this.columns.get(i).getNotNull()==false && entries.get(i)==null)
      {
        throw new IllegalArgumentException("The new data null value condition does not match the table");
      }
    }
  }

  public void typeconvert(String type, Entry entry) {
    try {
      switch (type) {
        case "INT":
          entry.value = Integer.valueOf(entry.value.toString());
          break;
        case "LONG":
          entry.value = Long.valueOf(entry.value.toString());
          break;
        case "FLOAT":
          entry.value = Float.valueOf(entry.value.toString());
          break;
        case "DOUBLE":
          entry.value = Double.valueOf(entry.value.toString());
          break;
        case "STRING":
          break;
        default:
          throw new IllegalArgumentException("The new data null value condition does not match the table");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("The new data null value condition does not match the table");
    }
  }

  public void insert(Row row) {
    validityCheckOfRow(row);
    //插入数据
    Entry newPrimaryKey = row.getEntries().get(primaryIndex);
    if (this.index.contains(newPrimaryKey)){
      throw new IllegalArgumentException("The primary key repeat");
    }
    this.index.put(newPrimaryKey, row);
  }

  public void delete(Row row) {
    validityCheckOfRow(row);
    //查找的到则删除，找不到报错
    Entry deletePrimaryKey = row.getEntries().get(primaryIndex);
    if (this.index.contains(deletePrimaryKey)) {
      this.index.remove(deletePrimaryKey);
    }
    else
    {
      throw new IllegalArgumentException("Column to delete was not found");
    }
  }

  public void update(Row row) {
    validityCheckOfRow(row);
    //查找并修改
    Entry updatePrimaryKey = row.getEntries().get(primaryIndex);
    if (this.index.contains(updatePrimaryKey)) {
      this.index.update(updatePrimaryKey,row);
    }
    else
    {
      insert(row);
    }
  }

  //新增查询函数
  public Row query(Entry queryPrimaryKey){
    if(index.contains(queryPrimaryKey)){
      return index.get(queryPrimaryKey);
    }
    else{
      throw new IllegalArgumentException("The query's primaryKey was not existing");
    }
  }

  private void serialize() {
    try{
      File folder=new File(this.databaseFolderPath);
      if(!folder.exists()){
        folder.mkdirs();
      }
      FileOutputStream serializeStream = new FileOutputStream(this.databaseFolderPath + this.tableName.toUpperCase() + ".data");
      ObjectOutputStream outStream = new ObjectOutputStream(serializeStream);
      Iterator<Row> rowIterator = iterator();
      while (rowIterator.hasNext())
      {
        outStream.writeObject(rowIterator.next());
      }
      outStream.flush();
      outStream.close();
      serializeStream.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private ArrayList<Row> deserialize(){
    ArrayList<Row> rows = new ArrayList<Row>();
    try {
      FileInputStream deserializeStream = new FileInputStream(this.databaseFolderPath + this.tableName.toUpperCase() + ".data");
      ObjectInputStream inStream = new ObjectInputStream(deserializeStream);
      while (true) {
        try {
          Row singleRow = (Row) inStream.readObject();
          rows.add(singleRow);
        } catch (Exception e) {
          break;
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return rows;
  }

  /**
   * 删除表，删除对应的元数据文件和数据文件文件
   * @author XuYihao
   */
  public void destroy(){
    File meta = new File(this.metaDir + this.metaFilename);
    File data = new File(this.dataFilePath);

    if (meta.exists()){
      meta.delete();
    }
    if (data.exists()){
      data.delete();
    }
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }

  public boolean examineAttr(String attrName){
    for(Column c: columns){
      if(c.name.equals(attrName)){
        return true;
      }
    }
    return false;
  }
}
