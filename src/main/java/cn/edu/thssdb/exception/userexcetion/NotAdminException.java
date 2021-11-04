package cn.edu.thssdb.exception.userexcetion;

public class NotAdminException extends UserSystemException{
    @Override
    public String getMessage() {
        return "You are not admin!";
    }
}
