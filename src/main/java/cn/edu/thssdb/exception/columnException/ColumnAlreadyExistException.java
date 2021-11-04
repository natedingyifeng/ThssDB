package cn.edu.thssdb.exception.columnException;

public class ColumnAlreadyExistException extends ColumnException {
  @Override
  public String getMessage() {
    return "Column already exists!";
  }
}
