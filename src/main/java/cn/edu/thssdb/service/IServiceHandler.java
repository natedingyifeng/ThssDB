package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.columnException.ColumnException;
import cn.edu.thssdb.exception.databaseException.DatabaseException;
import cn.edu.thssdb.exception.tableException.TableException;
import cn.edu.thssdb.exception.userexcetion.UserHasOnlineException;
import cn.edu.thssdb.exception.userexcetion.UserNotExistException;
import cn.edu.thssdb.exception.userexcetion.UserSystemException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.UserManager;
import cn.edu.thssdb.session.Session;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import org.apache.thrift.TException;
import org.omg.CORBA.UserException;

import java.util.*;

public class IServiceHandler implements IService.Iface {

  private UserManager userManager = UserManager.getInstance();

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    resp.setMsg("Successful get time!");
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    // 初始化账号、密码、响应
    String username = req.username;
    String password = req.password;
    ConnectResp resp = new ConnectResp();

    try {
      long sessionId = userManager.login(username, password);
      resp.setMsg("Successful connect!");
      resp.setSessionId(sessionId);
      resp.setStatus(new Status(Global.SUCCESS_CODE));
    } catch (UserSystemException e){
      resp.setStatus(new Status(Global.FAILURE_CODE));
      resp.setMsg(e.getMessage());
    }

    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    // TODO
    long sessionId = req.getSessionId();
    DisconnetResp resp = new DisconnetResp();

    try {
      userManager.logout(sessionId);
      resp.setMsg("Successful disconnect!");
      resp.setStatus(new Status(Global.SUCCESS_CODE));
    } catch (UserSystemException e){
      resp.setStatus(new Status(Global.FAILURE_CODE));
      resp.setMsg(e.getMessage());
    }

    return resp;
  }

  @Override
  public RegisterResp userRegister(RegisterReq req) throws TException{
    RegisterResp resp = new RegisterResp();

    try{
      userManager.register(req.getUsername(), req.getPassword(), req.getSessionId());
      resp.setStatus(new Status(Global.SUCCESS_CODE));
      resp.setMsg("Successful register!");
    }catch (UserSystemException e){
      resp.setMsg(e.getMessage());
      resp.setStatus(new Status(Global.FAILURE_CODE));
    }

    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    // TODO
    ExecuteStatementResp resp = new ExecuteStatementResp();
    long sessionId = req.getSessionId();
    if(userManager.hasOnlineUser(sessionId)){
          Session session = userManager.getSession(sessionId);
          String statment = req.statement;
          Pair<Global.STMT_TYPE, Object> statResult = null;
          try {
              statResult = session.parseSQLStatements(statment);
              System.out.println(statResult.getValue());
              if(statResult.getValue()!=null)
                  resp.setMsg(statResult.getValue().toString());
              else
                  resp.setMsg("");
              resp.setHasResult(false);
              resp.setStatus(new Status(Global.SUCCESS_CODE));
          } catch (DatabaseException | TableException | ColumnException e){
              resp.setMsg(e.getMessage());
              resp.setStatus(new Status(Global.FAILURE_CODE));
          } catch (Exception e){
            throw new RuntimeException("Statement Invalid or Error.");
          }
    }else{
      resp.setHasResult(false);
      resp.setIsAbort(true);
      resp.setStatus(new Status(Global.FAILURE_CODE));
    }
    return resp;
  }
}
