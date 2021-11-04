package cn.edu.thssdb.session;

import cn.edu.thssdb.exception.databaseException.DatabaseNotExistException;
import cn.edu.thssdb.exception.tableException.TableNotExistException;
import cn.edu.thssdb.query.ThssVisitor;
import cn.edu.thssdb.query.ThssParser;
import cn.edu.thssdb.query.base.ConditionItem;
import cn.edu.thssdb.query.base.FromItem;
import cn.edu.thssdb.query.base.Statement;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.transaction.LogData;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;



public class Session {
    long sessionId;
    String username;
    Manager manager;
    ThssParser parser = new ThssParser();
    public boolean isUserBeginTransaction;
    public ArrayList<String> lockedTableNames = new ArrayList<>();
    public ArrayList<LogData> logDatas = new ArrayList<>();
    public Object result = null;
    public Operations operation = new Operations();

    public Session(long sessionId, String username){
        this.sessionId = sessionId;
        this.username = username;
        this.manager = Manager.getInstance();
        this.isUserBeginTransaction = false;
        this.operation.manager = this.manager;
    }


    public Pair<Global.STMT_TYPE,Object> parseSQLStatements(String stat) throws InterruptedException {
        Pair<Global.STMT_TYPE,Object> pair = null;
        try {
            this.result = null;
            ArrayList<Statement> statements = parser.parseThss_stmts(stat);
            for(int i=0;i<statements.size();i++)
            {
                result = transactionBeforeStatment(statements.get(i));
                pair = new Pair<>(statements.get(i).type,result);
            }
        }catch(Exception e){
            throw e;
        }
        return pair;
    }

    void transactionOfReadLockStatement(Statement stat) throws InterruptedException {
        if (null!=operation.database) {
            switch (stat.type) {
                case SELECT: {
                    FromItem fromItem = (FromItem) stat.params[1];
                    try {
                        for (String tName : fromItem.table_name) {
                            Table table = operation.database.getTable(tName);
                            if (table == null) {
                                throw new TableNotExistException();
                            }
                            table.readLock.tryLock(20, TimeUnit.HOURS);
                        }
                    } catch (Exception e) {
                        throw e;
                    } finally {
                        result = exeTransactionOfNonLock(stat);
                        for (String tName : fromItem.table_name) {
                            Table table = operation.database.getTable(tName);
                            if (table == null) {
                                continue;
                            }
                            table.readLock.unlock();
                        }
                    }
                    break;
                }
            }
        }
        else
        {
            throw new DatabaseNotExistException();
        }
    }

    void transactionOfWriteLockStatement(Statement stat) throws InterruptedException {
        if (null!=operation.database)
        {
            String tName = (String)stat.params[0];
            if (isUserBeginTransaction){
                result = operation.database.transactionManager.toBeginAction(this,stat);
            }
            else{
                exeBeginTransaction();
                result = operation.database.transactionManager.toBeginAction(this,stat);
                exeCommit();
            }
        }
        else
        {
        throw new DatabaseNotExistException();
        }
    }

    void transactionOfNonLockStatement(Statement stat){
        if (null!=operation.database){
            try {
                result = exeTransactionOfNonLock(stat);
            }catch (Exception e){
                throw e;
            }
        }
        else
        {
            throw new DatabaseNotExistException();
        }
    }

    Object transactionBeforeStatment(Statement statement) throws InterruptedException {
        switch (statement.type){
            case CREATE_DATABASE:{
                operation.createDatabase((String)statement.params[0]);
                break;
            }
            case CREATE_TABLE:
            case DROP_TABLE:
            case SHOW_TABLE: {
                transactionOfNonLockStatement(statement);
                break;
            }
            case INSERT:
            case UPDATE:
            case DELETE: {
                transactionOfWriteLockStatement(statement);
                break;
            }
            case USE: {
                transactionOfUseDatabase(statement.params);
                break;
            }
            case SELECT: {
                transactionOfReadLockStatement(statement);
                break;
            }
            case BEGIN_TRANSACTION: {
                exeBeginTransaction();
                break;
            }
            case COMMIT: {
                exeCommit();
                break;
            }
            case ROLLBACK:{
                exeRollback(statement.params);
                break;
            }
            case SAVEPOINT:{
                exeSavepoint(statement.params);
                break;
            }
            default:
                break;
        }
        return result;
    }

    Object exeTransactionOfNonLock(Statement statment) {
        Object[] params = statment.params;
        switch (statment.type) {
            case SELECT:
                result = operation.selectTable((ArrayList<String>) params[0], (FromItem) params[1],
                        (ConditionItem) params[2]);
                break;
            case SHOW_TABLE:
                result = operation.showTable((String) params[0]);
                break;
            case CREATE_TABLE:
                result = operation.createTable((String) params[0], (ArrayList<Column>)params[1]);
                break;
            case DROP_TABLE:
                operation.dropTable((String) params[0]);
                result = "";
                break;
        }
        return result;
    }

    public ArrayList<LogData> execTranStmt(Statement stmt) {
        ArrayList<LogData> logs = new ArrayList<LogData>();
        Object[] params = stmt.params;
        switch (stmt.type) {
            case INSERT:
                LogData temp=operation.insertTable((String) params[0], (Row) params[1]);
                logs.add(temp);
                break;
            case DELETE:
                logs.addAll(operation.deleteTable((String) params[0], (ConditionItem) params[1]));
                break;
            case UPDATE:
                logs.addAll(
                        operation.updateTable((String) params[0], (String) params[1], (Entry) params[2], (ConditionItem) params[3]));
                break;
        }
        return logs;
    }


    void transactionOfUseDatabase(Object[] params){
        result = null;
        if(!isUserBeginTransaction){
            String databaseName = (String)params[0];
            if(manager.databases.containsKey(databaseName)){
                operation.database = manager.databases.get(databaseName);
            }
            else{
                throw new DatabaseNotExistException();
            }
        }
    }

    void exeBeginTransaction(){
        result = null;
        if(!isUserBeginTransaction){
            isUserBeginTransaction = true;
            logDatas.add(new LogData(LogData.LogStatType.BEGIN_TRANSACTION));
        }
    }

    void exeCommit(){
        result = null;
        if(isUserBeginTransaction){
            operation.database.transactionManager.toCommit(this);
            isUserBeginTransaction = false;
            operation.database.persist();
        }
    }

    void exeRollback(Object[] params){
        result = null;
        if(isUserBeginTransaction){
            operation.database.transactionManager.toRollback(this, (String)params[0]);
            isUserBeginTransaction = false;
        }
    }

    void exeSavepoint(Object[] params){
        result = null;
        if(isUserBeginTransaction){
            logDatas.add(new LogData(LogData.LogStatType.SAVEPOINT, (String)params[0]));
        }
    }
}
