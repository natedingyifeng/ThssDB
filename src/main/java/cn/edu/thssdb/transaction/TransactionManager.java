package cn.edu.thssdb.transaction;

import cn.edu.thssdb.exception.tableException.TableNotExistException;
import cn.edu.thssdb.query.base.Statement;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.UserManager;
import cn.edu.thssdb.session.Session;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;


public class TransactionManager {
  Database database;
  String dbName;
  WriteLog writeLog;
  String logPath;

  public TransactionManager(Database database){
    this.database = database;
    this.dbName = database.getDBName();
    this.writeLog = new WriteLog(dbName);
    this.logPath=Global.TRANSACTION_LOG_DIR+dbName+Global.TRANSACTION_LOG_FILE;
    this.writeLog.start();
  }

  public TransactionManager(Database database, boolean recover){
    this.database = database;
    this.dbName = database.getDBName();
    this.writeLog = new WriteLog(dbName);
    this.logPath=Global.TRANSACTION_LOG_DIR+dbName+Global.TRANSACTION_LOG_FILE;
    if(recover){
      ArrayList<LogData> logDatas = getLogDataBefore();
      if (logDatas.size() > 0){
        recoverLogDatas(logDatas);
        database.persist();
      }
      File file = new File(this.logPath);
      if (file.isFile()){
        file.delete();
      }
    }
    this.writeLog.start();
  }

  public Object toBeginAction(Session session, Statement statement) throws InterruptedException {
    ArrayList<LogData> logDatas;
    String tableName = (String)statement.params[0];
    Table table = session.operation.database.getTable(tableName);
    if (table != null) {
      if(session.lockedTableNames.contains(tableName)){
        logDatas = session.execTranStmt(statement);
        session.logDatas.addAll(logDatas);
      }
      else{
        table.writeLock.tryLock(20, TimeUnit.HOURS);
        session.lockedTableNames.add(tableName);
        try {
          logDatas = session.execTranStmt(statement);
          session.logDatas.addAll(logDatas);
        }
        catch (Exception e) {
          table.writeLock.unlock();
          session.lockedTableNames.remove(tableName);
          throw e;
        }
      }
    }
    else
    {
      throw new TableNotExistException();
    }
    return logDatas.toString();
  }

  public void toCommit(Session session){
    session.logDatas.clear();
    CopyOnWriteArrayList<LogData> logDatas = new CopyOnWriteArrayList<>(session.logDatas);
    writeLog.put(logDatas);
    for(int i=0;i<session.lockedTableNames.size();i++){
      Table table = database.getTable(session.lockedTableNames.get(i));
      table.writeLock.unlock();
    }
    session.lockedTableNames.clear();
  }

  public void toRollback(Session session, String point){
    session.logDatas.add(new LogData(LogData.LogStatType.ROLLBACK, point));
    CopyOnWriteArrayList<LogData> logDatas = new CopyOnWriteArrayList<>(session.logDatas);
    writeLog.put(logDatas);
    rollbackLogDatas(session.logDatas, session.logDatas.size() - 1);
    for(int i=0;i<session.lockedTableNames.size();i++){
      Table table = database.getTable(session.lockedTableNames.get(i));
      table.writeLock.unlock();
    }
    session.lockedTableNames.clear();
  }

  private ArrayList<LogData> getLogDataBefore(){
    ArrayList<LogData> logDatas = new ArrayList<>();
    try {
      FileInputStream fileStream = new FileInputStream(this.logPath);
      ObjectInputStream in = new ObjectInputStream(fileStream);
      while(true){
        try{
          LogData logData = (LogData) in.readObject();
          logDatas.add(logData);
        }catch(IOException | ClassNotFoundException e) {
          break;
        }
      }
      in.close();
      fileStream.close();
    } catch (IOException e) {
    }
    return logDatas;
  }

  private void recoverLogDatas(ArrayList<LogData> logDatas){
    for(int i = 0; i < logDatas.size(); i++) {
      LogData logData = logDatas.get(i);
      switch (logData.logStatType) {
        case INSERT:{
          Table table = database.getTable(logData.tableName);
          if (table == null) {
            continue;
          }
          table.insert(logData.row);
          break;
        }
        case UPDATE:{
          Table table = database.getTable(logData.tableName);
          if (table == null) {
            continue;
          }
          logData.row.updateEntery(logData.position, logData.varOld);
          table.delete(logData.row);
          logData.row.updateEntery(logData.position, logData.varNew);
          table.insert(logData.row);
          break;
        }
        case DELETE:{
          Table table = database.getTable(logData.tableName);
          if (table == null) {
            continue;
          }
          table.delete(logData.row);
          break;
        }
        case BEGIN_TRANSACTION:
        case COMMIT:
          break;
        case ROLLBACK:{
          rollbackLogDatas(logDatas, i);
          break;
        }
      }
    }
  }

  private void rollbackLogDatas(ArrayList<LogData> logDatas, int begin){
    String pos = logDatas.get(begin).tableName;
    for(int i = begin - 1; i >= 0; i--) {
      LogData logData = logDatas.get(i);
      boolean mark = false;
      switch(logData.logStatType){
        case INSERT:{
          Table table = database.getTable(logData.tableName);
          if (table != null) {
            table.delete(logData.row);
          }
          break;
        }
        case UPDATE:{
          Table table = database.getTable(logData.tableName);
          if (table != null) {
            logData.row.updateEntery(logData.position, logData.varNew);
            table.delete(logData.row);
            logData.row.updateEntery(logData.position, logData.varOld);
            table.insert(logData.row);
          }
          break;
        }
        case DELETE:{
          Table table = database.getTable(logData.tableName);
          if (table != null) {
            table.insert(logData.row);
          }
          break;
        }
        case BEGIN_TRANSACTION:{
          mark = true;
          break;
        }
        case SAVEPOINT:
          if (logData.tableName.equals(pos)){
            mark = true;
          }
          break;
      }
      if (mark) {
        break;
      }
    }
  }

  public void destroy(){
//    writeLog.interrupt();
//    try {
//      writeLog.join();
//    } catch (InterruptedException ignored) {
//    }
//    File logFile = new File(this.logPath);
//    if (logFile.isFile())
//      logFile.delete();
  }
}
