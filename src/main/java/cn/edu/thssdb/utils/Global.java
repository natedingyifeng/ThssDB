package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;

  // 终端语句
  public static String CLI_PREFIX = "ThssDB>";
  public static final String SHOW_TIME =    "show time;";
  public static final String QUIT =         "quit;";
  public static final String DISCONNECT =   "disconnect;";
  public static final String CONNECT =      "connect;";
  public static final String EXECUTE =      "execute;";
  public static final String HELP =         "help;";
  public static final String CREATE_USER =  "create user;";
  public static final String REGISTER =     "register;";

  // 元数据文件
  public static final String USER_METADATA_FILE = "user.meta";
  public static final String USER_METADATA_DIR = "./src/data/";

  public static final String MANAGER_METADATA_FILE = "manager.meta";
  public static final String MANAGER_METADATA_DIR = "./src/data/";

  public static final String TRANSACTION_LOG_FILE = ".logData";
  public static final String TRANSACTION_LOG_DIR = "./src/data/";

  public static final String TABLE_DIR = "./src/data";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  // 管理员用户账号密码（暂时）
  public static final String USERNAME = "ThssDB";
  public static final String PASSWORD = "ThssDB";

  // PageFile
  public static final int PAGE_SIZE = 1024;

  public static final int MAX_LENGTH = 10;

  public static enum STMT_TYPE{
    CREATE_TABLE,
    ALTER_TABLE,
    CREATE_DATABASE,
    DELETE,
    DROP_TABLE,
    INSERT,
    SELECT,
    SHOW_TABLE,
    UPDATE,
    BEGIN_TRANSACTION,
    COMMIT,
    ROLLBACK,
    SAVEPOINT,
    USE
  }

  public static enum COLUMN_TYPE {
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING
  }

//  public enum StatementType{
//    BEGIN_TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT,
//    CREATE_DATABASE, USE,
//    CREATE_TABLE, DROP_TABLE, SHOW_TABLE, ALTER_TABLE_ADD, ALTER_TABLE_DROP,
//    INSERT, DELETE, UPDATE, SELECT
//  }
}
