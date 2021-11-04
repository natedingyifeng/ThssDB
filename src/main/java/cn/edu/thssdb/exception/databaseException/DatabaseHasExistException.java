package cn.edu.thssdb.exception.databaseException;

public class DatabaseHasExistException extends DatabaseException{
    @Override
    public String getMessage() {
        return "Database has already exist!";
    }
}
