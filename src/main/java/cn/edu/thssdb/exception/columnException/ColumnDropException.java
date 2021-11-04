package cn.edu.thssdb.exception.columnException;

public class ColumnDropException extends RuntimeException {
    @Override
    public String getMessage(){ return "Exception: Column drop failed!"; }
}
