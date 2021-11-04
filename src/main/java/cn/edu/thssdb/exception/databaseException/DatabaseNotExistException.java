package cn.edu.thssdb.exception.databaseException;

public class DatabaseNotExistException extends DatabaseException{
    @Override
    public String getMessage() {
        return "Database do not exist!";
    }
}
