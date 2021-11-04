package cn.edu.thssdb.query.base;

import java.util.ArrayList;

public class FromItem {
    public ArrayList<String> table_name;
    public ConditionItem condition = null;
    public String ComparisonOperator = null;
    public FromItem(ArrayList<String> name){
        this.table_name = name;
    }
    public FromItem(ArrayList<String> name, ConditionItem condition){
        this.table_name = name;
        this.condition = condition;
        ComparisonOperator = "=";
    }
}
