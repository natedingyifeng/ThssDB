package cn.edu.thssdb.exception.tableException;

public class TableAlreadyExistException extends TableException{
  @Override
  public String getMessage() {
    return "Table already exists!";
  }
}
