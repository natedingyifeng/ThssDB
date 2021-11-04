package cn.edu.thssdb.schema;

public class User {
    private String username;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    private String password;
    public enum Permission {ADMIN, USER};
    private Permission permission;

    public User(String username, String password, String permission){
        this.username = username;
        this.password = password;
        this.permission = stringToPermission(permission);
    }

    private static Permission stringToPermission(String perm){
        if(perm.equals("ADMIN")) return Permission.ADMIN;
        return Permission.USER;
    }

    private static String permissionToString(Permission perm){
        if(perm == Permission.ADMIN) return "ADMIN";
        return "USER";
    }

    public String getPermission(){
        return permissionToString(permission);
    }

    public boolean isAdmin(){
        return permission == Permission.ADMIN;
    }

    public String toString(){
        return username + ' ' + password + ' ' + permissionToString(permission);
    }

}
