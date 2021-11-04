package cn.edu.thssdb.query.base;

public class ExpressionItem {
    public String item_A = null;
    public String item_B = null;
    public String ComparisonOperator = null;
    public ExpressionItem(String A, String B, String operator)
    {
        this.item_A = A;
        this.item_B = B;
        this.ComparisonOperator = operator;
    }
}
