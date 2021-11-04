package cn.edu.thssdb.exception.userexcetion;

public class UserHasOnlineException extends UserSystemException{
    @Override
    public String getMessage() {
        return "User has already online!";
    }
}
