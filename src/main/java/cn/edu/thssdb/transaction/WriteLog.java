package cn.edu.thssdb.transaction;
import cn.edu.thssdb.exception.WriteFileException;
import cn.edu.thssdb.utils.Global;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class WriteLog extends Thread {
    private String dbName;
    private String outPath;
    private LinkedBlockingQueue<CopyOnWriteArrayList<LogData>> logDataQueue;
    public WriteLog(String dbName){
        this.dbName = dbName;
        this.logDataQueue = new LinkedBlockingQueue();
    }

    public void put(CopyOnWriteArrayList<LogData> logDatas) {
        while(true){
            try{
                logDataQueue.put(logDatas);
            }
            catch (InterruptedException e){
                continue;
            }
            break;
        }
    }

    @Override
    public void run(){
        try {
            if(!dbName.equals(null))
            {
                outPath= Global.TRANSACTION_LOG_DIR+ dbName+Global.TRANSACTION_LOG_FILE;
                FileOutputStream file = new FileOutputStream(outPath);
                ObjectOutputStream out = new ObjectOutputStream(file);
                while(true) {
                    try {
                        CopyOnWriteArrayList<LogData> logDatas = this.logDataQueue.take();
                        for (int i=0;i<logDatas.size();i++) {
                            if (logDatas.get(i) != null)
                            {
                                out.writeObject(logDatas.get(i));
                            }
                        }
                    } catch (InterruptedException e) {
                        ArrayList<CopyOnWriteArrayList<LogData>> logDatas = new ArrayList<>();
                        this.logDataQueue.drainTo(logDatas);
                        for (int i=0;i<logDatas.size();i++) {
                            if (logDatas.get(i) != null)
                            {
                                out.writeObject(logDatas.get(i));
                            }
                        }
                        break;
                    }
                }
                out.close();
                file.close();
            }
        } catch (IOException e) {
            throw new WriteFileException();
        }
    }
}