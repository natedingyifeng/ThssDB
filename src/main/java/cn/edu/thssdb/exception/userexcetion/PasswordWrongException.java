package cn.edu.thssdb.exception.userexcetion;

public class PasswordWrongException extends UserSystemException{
    @Override
    public String getMessage() {
        return "Password wrong!";
    }
}
