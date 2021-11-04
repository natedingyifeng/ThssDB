package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.userexcetion.*;
import cn.edu.thssdb.session.Session;
import cn.edu.thssdb.utils.Global;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UserManager {
    public static Set<User> users = new HashSet<>();
    public static Map<Long, String> onlineUsers = new HashMap<>(); // 记录sessionId和username的对应关系
    public static Map<Long, Session> onlineUsersSession = new HashMap<>();
    public static UserManager getInstance(){return UserManagerHolder.INSTANCE;}
    private final Random random = new Random();
    private MetaData metaData = new MetaData(Global.USER_METADATA_DIR, Global.USER_METADATA_FILE);

    public UserManager(){
        // 从元数据中读取用户数据
        List<String[]> userList = metaData.readMetaData();
        for(String[] line : userList){
            users.add(new User(line[0], line[1], line[2]));
        }
        //
        String username = "admin";
        String password = "admin";
        User userAdmin=new User(username,password,"ADMIN");
        UserManager.users.add(userAdmin);
    }

    /**
     * 通过用户名密码注册用户。
     * @param username
     * @param password
     * @param sessionId 执行操作的用户的sessionId，只有管理员能够注册
     */
    public void register(String username,String password, long sessionId){
        // 判断用户是否已经存在，注册人员是否是管理员
        if(getUser(username) != null)
            throw new UserHasRegisterException();
        if(!onlineUsers.containsKey(sessionId))
            throw new UserNotOnlineException();
        if(!getUser(onlineUsers.get(sessionId)).isAdmin())
            throw new NotAdminException();

        User resUser = new User(username, password, "USER");
        users.add(resUser);

        // DONE 持久化存储用户
        metaData.appendMetaData(resUser.toString());
    }

    /**
     * 根据用户名寻找用户
     * @param username
     * @return 返回找到的用户，找不到则返回null
     * @author XuYihao
     */
    public User getUser(String username){
        for(User user : users){
            if(user.getUsername().equals(username)){
                return user;
            }
        }
        return null;
    }

    /**
     * 用户使用用户名密码登录
     * @param username
     * @param password
     * @return 返回生成的sessionId
     * @author XuYihao
     */
    public long login(String username, String password)
        throws UserHasOnlineException, PasswordWrongException, UserNotExistException
    {
        if(getUser(username) == null) // 用户不存在
            throw new UserNotExistException();
        if(!getUser(username).getPassword().equals(password)) // 密码错误
            throw new PasswordWrongException();
        if(onlineUsers.containsValue(username)) // 用户已登录
            throw new UserHasOnlineException();

        long sessionId = Math.abs(random.nextLong());
        while(onlineUsers.containsKey(sessionId)){
            sessionId = Math.abs(random.nextLong());
        }
        Session session=new Session(sessionId,username);
        onlineUsers.put(sessionId,username);
        onlineUsersSession.put(sessionId, session);
        return sessionId;
    }

    /**
     * 通过sessionId登出
     * @param sessionId
     * @author XuYihao
     */
    public void logout(long sessionId){
        if(!onlineUsers.containsKey(sessionId))
            throw new UserNotOnlineException();

        onlineUsers.remove(sessionId);
        onlineUsersSession.remove(sessionId);
    }

    public boolean hasOnlineUser(long sessionId){
        if(!onlineUsers.containsKey(sessionId)){
            return false;
        }
        return true;
    }

    private static class UserManagerHolder{
        private static final UserManager INSTANCE = new UserManager();
        private UserManagerHolder(){}
    }

    public Session getSession(long sessionId){
         if(!onlineUsers.containsKey(sessionId))
                throw new UserNotOnlineException();
        return onlineUsersSession.get(sessionId);
    }

}
