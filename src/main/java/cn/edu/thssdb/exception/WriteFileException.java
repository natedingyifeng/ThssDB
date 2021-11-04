package cn.edu.thssdb.exception;

public class WriteFileException extends RuntimeException{
  @Override
  public String getMessage() {
    return "Write File Wrongly!";
  }
}
