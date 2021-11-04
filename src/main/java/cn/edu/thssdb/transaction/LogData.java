package cn.edu.thssdb.transaction;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.io.Serializable;

public class LogData implements Serializable {
    public enum LogStatType{
        BEGIN_TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT, INSERT, UPDATE, DELETE,
    }
    public LogStatType logStatType;
    public String tableName;
    public Row row = null;
    public Entry varOld = null;
    public Entry varNew = null;
    public int position=0;

    public LogData(LogStatType logStatType, String tableName, Row row, Entry varOld, Entry varNew, int position){
        this.logStatType = logStatType;
        this.tableName = tableName;
        this.row = row;
        this.varOld = varOld;
        this.varNew = varNew;
        this.position = position;
    }

    public LogData(LogStatType logStatType, String tableName, Row row){
        this.logStatType = logStatType;
        this.tableName = tableName;
        this.row = row;
    }

    public LogData(LogStatType logStatType, String point){
        this.logStatType = logStatType;
        this.tableName = point;
    }

    public LogData(LogStatType logStatType){
        this.logStatType = logStatType;
    }
}
