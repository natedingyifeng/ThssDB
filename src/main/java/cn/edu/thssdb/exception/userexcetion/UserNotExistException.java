package cn.edu.thssdb.exception.userexcetion;

public class UserNotExistException extends UserSystemException{
    @Override
    public String getMessage() {
        return "User doesn't exist!";
    }
}
