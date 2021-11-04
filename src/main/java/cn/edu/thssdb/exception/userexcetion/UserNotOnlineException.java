package cn.edu.thssdb.exception.userexcetion;

public class UserNotOnlineException extends UserSystemException{
    @Override
    public String getMessage() {
        return "You are not online!";
    }
}
