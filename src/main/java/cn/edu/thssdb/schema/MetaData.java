package cn.edu.thssdb.schema;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MetaData {
    private String filename; // 文件名
    private String fileDir; // 文件夹路径
    private String filepath; // 文件路径
    private String splitString = " ";


    public MetaData(String fileDir, String filename){
        this.fileDir = fileDir;
        this.filename = filename;
        this.filepath = fileDir + filename;
        File file = new File(filepath);
        File dir = new File(fileDir);
        // 如果没有对应文件，则创建新文件
        if(!dir.isDirectory()){
            dir.mkdirs();
        }
        if(!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从文件中读取元数据
     * @return 读取到的List
     */
    public List<String[]> readMetaData(){
        List<String[]> metaData = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(filepath)));
            String line;
            while((line = reader.readLine()) != null){
                metaData.add(line.split(splitString));
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        return metaData;
    }

    /**
     * 从文件中一行一行地读取元数据
     * @return 读取到的List
     */
    public List<String> readMetaDataLine(){
        List<String> metaData = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(filepath)));
            String line;
            while((line = reader.readLine()) != null){
                metaData.add(line);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        return metaData;
    }

    /**
     * 重写整个文件的元数据
     * @param metaData 整个MetaData的List
     */
    public void writeMetaData(List<String> metaData){
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filepath)));
            for(String line : metaData){
                writer.write(line + '\n');
            }
            writer.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 重写整个文件的元数据
     * @param metaData 整个MetaData的Set
     */
    public void writeMetaData(Set<String> metaData){
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filepath)));
            for(String line : metaData){
                writer.write(line + '\n');
            }
            writer.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }


    /**
     * 追加写入元数据
     * @param metaData 追加的数据列表
     */
    public void appendMetaData(List<String> metaData){
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filepath), true));
            for(String line : metaData){
                writer.append(line + '\n');
            }
            writer.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 追加写入元数据
     * @param metaData 追加写入的元数据行
     */
    public void appendMetaData(String metaData){
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filepath), true));
            writer.append(metaData + '\n');
            writer.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public String getSplitString() {
        return splitString;
    }

    public void setSplitString(String splitString) {
        this.splitString = splitString;
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public String getFileDir() {
        return fileDir;
    }

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
    }

}
