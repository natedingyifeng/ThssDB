package cn.edu.thssdb.exception.tableException;

public class TableNotExistException extends TableException {
    @Override
    public String getMessage() {
        return "Table do not exist!";
    }
}
