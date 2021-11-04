package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.User;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  private static boolean isConnected = false;
  private static long sessionId;
  private static String username;
  private static String password;

  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TSocket(host, port);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX);
        String msg = SCANNER.nextLine();
        long startTime = System.currentTimeMillis();
        switch (msg.trim()) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            break;
          case Global.CONNECT:
            print("Please enter username: ");
            username = SCANNER.nextLine().trim();
            print("Please enter password: ");
            password = SCANNER.nextLine().trim();
            connect();
            break;
          case Global.DISCONNECT:
            disconnect();
            break;
          case Global.HELP:
            showHelp();
            break;
          case Global.REGISTER:
            print("Please enter username: ");
            username = SCANNER.nextLine().trim();
            print("Please enter password: ");
            password = SCANNER.nextLine().trim();
            register(username,password,sessionId);
            break;
          default:
            if(isConnected) {
              execute(msg.trim());
            }else{
              print("Haven't Connected Yet!\n");
            }
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
        .argName(HELP_NAME)
        .desc("Display help information(optional)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
        .argName(HOST_NAME)
        .desc("Host (optional, default 127.0.0.1)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
        .argName(PORT_NAME)
        .desc("Port (optional, default 6667)")
        .hasArg(false)
        .required(false)
        .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  private static void connect(){

    ConnectReq req = new ConnectReq(username, password);
    try {
      ConnectResp resp = client.connect(req);
      if (resp.getStatus().code == Global.SUCCESS_CODE){
        isConnected = true;
        sessionId = resp.getSessionId();
        println("Connection succeeds.\n");
      }else{
        isConnected = false;
        sessionId = -1;
        println("Connection fails.\n");
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void disconnect(){
    DisconnetReq req = new DisconnetReq();
    req.setSessionId(sessionId);
    try{
      DisconnetResp resp = client.disconnect(req);
      if (resp.getStatus().getCode() == Global.SUCCESS_CODE){
        isConnected = false;
        sessionId = -1;
        println("Disconnection succeeds.\n");
      }else {
        isConnected = false;
        sessionId = -1;
        println("Disconnection fails.\n");
      }
    }catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void register(String username, String password, long sessionId) {
    RegisterReq req = new RegisterReq(sessionId, username, password);
    try {
      RegisterResp resp = client.userRegister(req);
      if (resp.getStatus().getCode() == Global.SUCCESS_CODE) {
        print("Successful register!");
      } else {
        print(resp.getMsg());
      }
    } catch (TException e) {
      print(e.getMessage());
      logger.error(e.getMessage());
    }
  }

  private static void execute(String statement){
    ExecuteStatementReq req = new ExecuteStatementReq(sessionId,statement);
    try{
      ExecuteStatementResp resp = client.executeStatement(req);
      if(resp.getStatus().code == Global.SUCCESS_CODE){
        println(resp.getMsg());
      }
      else{
        println("Invalid SQL Statement!");
      }
    }catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  static void showHelp() {
    println("Help:\n" +
            "connect;     账号密码连接。\n" +
            "disconnect;  断开连接。\n" +
            "show time;   显示时间。\n" +
            "quit;        退出。\n");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }

}
