package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.databaseException.DatabaseException;
import cn.edu.thssdb.exception.databaseException.DatabaseHasExistException;
import cn.edu.thssdb.exception.databaseException.DatabaseNotExistException;
import cn.edu.thssdb.exception.databaseException.DatabaseNotOnlineException;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  public HashMap<String, Database> databases;
  private static HashMap<String, Integer> onlineDatabases;
  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  /**
   * Manager 元数据格式（每行）
   * 数据库名称
   */
  private static MetaData metaData;

  public Manager() {
    // DONE
    databases = new HashMap<>();
    metaData = new MetaData(Global.MANAGER_METADATA_DIR, Global.MANAGER_METADATA_FILE);
    onlineDatabases = new HashMap<>();
    recoverMetaData();
  }

  public void transactionOfCreateDatabase(String databaseName) {
    databases.put(databaseName, new Database(databaseName));
    updateMetaData();
  }

  public void deleteDatabase(String databaseName) {
    // DONE
    if(!databases.containsKey(databaseName))
      throw new DatabaseNotExistException();

    databases.remove(databaseName).destroy();
    if (onlineDatabases.containsKey(databaseName))
      onlineDatabases.remove(databaseName);
    updateMetaData();
  }

  public void switchDatabase(String databaseName) {
    // DONE
    if(!databases.containsKey(databaseName))
      throw new DatabaseNotExistException();

    if(onlineDatabases.containsKey(databaseName)){
      onlineDatabases.replace(databaseName, onlineDatabases.get(databaseName) + 1);
    }else{
      onlineDatabases.put(databaseName, 1);
    }
    updateMetaData();
  }

  public void quitDatabase(String databaseName){
    if(!databases.containsKey(databaseName))
      throw new DatabaseNotExistException();

    if(onlineDatabases.containsKey(databaseName)){
      if(onlineDatabases.get(databaseName) == 1){
        onlineDatabases.remove(databaseName);
        databases.get(databaseName).quit();
      }else{
        onlineDatabases.replace(databaseName, onlineDatabases.get(databaseName) - 1);
      }
    }else{
      throw new DatabaseNotOnlineException();
    }
  }

  /**
   * 将MetaData的数据写入文件。
   */
  private void updateMetaData(){
    metaData.writeMetaData(databases.keySet());
  }

  /**
   * 从MetaData文件中读取数据，并恢复到databases中。
   */
  private void recoverMetaData(){
    List<String[]> databaseList = metaData.readMetaData();
    for(String databaseInfo[] : databaseList){
      databases.put(databaseInfo[0], new Database(databaseInfo[0], true));
    }
  }

  public boolean contain(String databaseName){
    return databases.containsKey(databaseName);
  }

  public boolean databaseOnline(String databaseName){
    return onlineDatabases.containsKey(databaseName);
  }

  public Database getDatabase(String databaseName){
    return databases.get(databaseName);
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {
    }
  }
}
