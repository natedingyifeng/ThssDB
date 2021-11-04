package cn.edu.thssdb.exception.databaseException;

public class DatabaseNotOnlineException extends DatabaseException{
    @Override
    public String getMessage() {
        return "Database is not online!";
    }
}
