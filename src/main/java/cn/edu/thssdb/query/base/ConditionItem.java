package cn.edu.thssdb.query.base;

public class ConditionItem {
    public Object item_A = null;
    public Object item_B = null;
    public String ComparisonOperator = null;
//    private int type = 0;
//    private boolean is_bool = false;
//    private Object value_bool = null;
    public ConditionItem(Object A, Object B, String operator)
    {
        this.item_A = A;
        this.item_B = B;
        this.ComparisonOperator = operator;
    }
    public ConditionItem(Object flag)
    {
        this.item_A = flag;
    }
}
