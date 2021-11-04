package cn.edu.thssdb.exception.tableException;

public class TableAttrNotExistException extends TableException{
    private String tableName;
    private String attrName;
    public TableAttrNotExistException(String tableName, String attrName){
        this.tableName = tableName;
        this.attrName = attrName;
    }
    @Override
    public String getMessage(){ return String.format(String.format("Table %s doesn't have the attr named %s",tableName,attrName)); }
}
