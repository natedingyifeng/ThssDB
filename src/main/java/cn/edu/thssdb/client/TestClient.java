package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.User;
import cn.edu.thssdb.schema.UserManager;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestClient {

  private static final Logger logger = LoggerFactory.getLogger(TestClient.class);

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;

  //定义全局变量，存放所有文件夹下的文档
  public static void deleteFiles(String path) {
    File file = new File(path);
    if(file.exists()){
      File[] files = file.listFiles();
      for(File file2 : files){
        //若是文件夹，递归调用，往下继续遍历
        if(file2.isDirectory()){
          //遍历子文件夹的绝对路径下的所有文件
          deleteFiles(file2.getAbsolutePath());
          file2.delete();
        }else{
          //若是文档，非文件夹，加入List
          file2.delete();
        }
      }
    }
  }

  public static void main(String[] args) {
    try {
      //删除上一次测试的本地持续化文件防止报错
      deleteFiles("./src/data");

      transport = new TSocket(Global.DEFAULT_SERVER_HOST, Global.DEFAULT_SERVER_PORT);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      //登录管理员
      String username = "admin";
      String password = "admin";
      ConnectReq connectReq = new ConnectReq(username, password);
      long adminSessionId=client.connect(connectReq).getSessionId();
      //注册新用户
      String newUsername = "test";
      String newPassword = "test";
      RegisterReq registerReq = new RegisterReq(adminSessionId,newUsername, newPassword);
      client.userRegister(registerReq);
      //注销
      DisconnetReq disconnetReq = new DisconnetReq(adminSessionId);
      client.disconnect(disconnetReq);
      //登录新用户
      ConnectReq connectReqNew = new ConnectReq(newUsername, newPassword);
      long curSessionId=client.connect(connectReqNew).getSessionId();
      //创建数据库
      String statement = "create database testdb;";
      ExecuteStatementReq req = new ExecuteStatementReq(curSessionId, statement);
      ExecuteStatementResp resp = client.executeStatement(req);
      if (resp.getStatus().code == Global.SUCCESS_CODE) {
        println("Create Database Successfully!");
      } else {
        println("Create Database Unsuccessfully!");
      }
//      //使用数据库
//      statement = "use testdb;";
//      req = new ExecuteStatementReq(curSessionId, statement);
//      resp = client.executeStatement(req);
//      if (resp.getStatus().code == Global.SUCCESS_CODE) {
//        println("Use Database Successfully!");
//      } else {
//        println("Use Database Unsuccessfully!");
//      }
      //创建表格
      statement = "create table table1 (test1 String(32),test2 Int not null, PRIMARY KEY(test2));";
      req = new ExecuteStatementReq(curSessionId, statement);
      resp = client.executeStatement(req);
      if (resp.getStatus().code == Global.SUCCESS_CODE) {
        println("Create Table Successfully!");
        println(resp.getMsg());
      } else {
        println("Create Table Unsuccessfully!");
        println(resp.getMsg());
      }
      statement = "create table table2 (test1 String(32),test2 Int not null, PRIMARY KEY(test2));";
      req = new ExecuteStatementReq(curSessionId, statement);
      resp = client.executeStatement(req);
      if (resp.getStatus().code == Global.SUCCESS_CODE) {
        println("Create Table Successfully!");
        println(resp.getMsg());
      } else {
        println("Create Table Unsuccessfully!");
        println(resp.getMsg());
      }
      //插入数据
      boolean state = true;
      String[] statements = new String[]{
              "insert into table1 values ('str1', 1);",
              "insert into table1 values ('str2', 2);",
              "insert into table1 values ('str3', 3);",
              "insert into table1 values ('str4', 4);",
      };
      for (String stat : statements) {
        req = new ExecuteStatementReq(curSessionId, stat);
        resp = client.executeStatement(req);
        if (resp.getStatus().code == Global.FAILURE_CODE) {
          state = false;
        }
      }
      if (state) {
        println("Insert Successfully!");
        println(resp.getMsg());
      } else {
        println("Insert Unsuccessfully!");
        println(resp.getMsg());
      }
      statements = new String[]{
              "insert into table2 values ('str1', 1);",
              "insert into table2 values ('str2', 2);",
              "insert into table2 values ('str3', 3);",
              "insert into table2 values ('str4', 4);",
      };
      for (String stat : statements) {
        req = new ExecuteStatementReq(curSessionId, stat);
        resp = client.executeStatement(req);
        if (resp.getStatus().code == Global.FAILURE_CODE) {
          state = false;
        }
      }
      if (state) {
        println("Insert Successfully!");
        println(resp.getMsg());
      } else {
        println("Insert Unsuccessfully!");
        println(resp.getMsg());
      }

      statements = new String[]{
              "DELETE FROM table2 WHERE test1 = 'str2'"
      };
      for (String stat : statements) {
        req = new ExecuteStatementReq(curSessionId, stat);
        resp = client.executeStatement(req);
        if (resp.getStatus().code == Global.FAILURE_CODE) {
          state = false;
        }
      }
      if (state) {
        println("Delete Successfully!");
        println(resp.getMsg());
      } else {
        println("Delete Unsuccessfully!");
        println(resp.getMsg());
      }
      statements = new String[]{
              "UPDATE table1 SET test1 = 'str10' WHERE test2 = 1"
      };
      for (String stat : statements) {
        req = new ExecuteStatementReq(curSessionId, stat);
        resp = client.executeStatement(req);
        if (resp.getStatus().code == Global.FAILURE_CODE) {
          state = false;
        }
      }
      if (state) {
        println("Update Successfully!");
        println(resp.getMsg());
      } else {
        println("Update Unsuccessfully!");
        println(resp.getMsg());
      }
      //查询
      statement = "select test1,test2 from table1 where test1='str1'";
      req = new ExecuteStatementReq(curSessionId, statement);
      resp = client.executeStatement(req);
//      String results = "[[str1, 1], [str2, 2], [str3, 3], [str4, 4]]";
//      List<List<String>> result = resp.getRowList();
//      String resStr=result.toString();
      println("Query Successfully!");
      println(resp.getMsg());
      statement = "select test1,test2 from table1 WHERE test2 > 2;";
      req = new ExecuteStatementReq(curSessionId, statement);
      resp = client.executeStatement(req);
      println("Query Successfully!");
      println(resp.getMsg());
      statement = "SELECT table1.test2, table2.test2 FROM table1 JOIN table2 ON table1.test1 = table2.test1 WHERE test2 <= 3";
      req = new ExecuteStatementReq(curSessionId, statement);
      resp = client.executeStatement(req);
      println("Query Successfully!");
      println(resp.getMsg());
//      if (resStr.equals(results)) {
//        println("Query Successfully!");
//      } else {
//        println("Query Unsuccessfully!");
//      }
      //注销
      disconnetReq = new DisconnetReq(curSessionId);
      client.disconnect(disconnetReq);
      transport.close();
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}