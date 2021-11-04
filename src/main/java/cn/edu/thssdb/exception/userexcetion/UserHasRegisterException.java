package cn.edu.thssdb.exception.userexcetion;

public class UserHasRegisterException extends UserSystemException{
    @Override
    public String getMessage() {
        return "User has already register!";
    }
}
