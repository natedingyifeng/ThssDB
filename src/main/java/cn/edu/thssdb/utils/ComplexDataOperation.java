package cn.edu.thssdb.utils;

import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.base.ConditionItem;
import cn.edu.thssdb.query.base.ExpressionItem;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import java.util.*;

public class ComplexDataOperation {
    public QueryTable getJoinRow(Table table1, Table table2, ExpressionItem expression) {
        if (!expression.ComparisonOperator.equals("=")) {
            return null;
        }
        String[] attrName1 = ((String) expression.item_A).split("\\.");
        String[] attrName2 = ((String) expression.item_B).split("\\.");
        MetaInfo metaInfo1 = new MetaInfo(table1.tableName, table1.columns);
        MetaInfo metaInfo2 = new MetaInfo(table2.tableName, table2.columns);
        int checkPosition1 = -1;
        int checkPosition2 = -1;
        if (attrName1[0].equals(table1.tableName)) {
            checkPosition1 = metaInfo1.columnFind(attrName1[1]);
        } else if (attrName1[0].equals(table2.tableName)) {
            checkPosition2 = metaInfo2.columnFind(attrName1[1]);
        }
        if (attrName2[0].equals(table1.tableName)) {
            checkPosition1 = metaInfo1.columnFind(attrName2[1]);
        } else if (attrName2[0].equals(table2.tableName)) {
            checkPosition2 = metaInfo2.columnFind(attrName2[1]);
        }
        QueryResult queryResult = new QueryResult();
        queryResult.addMetaInfo(metaInfo1);
        queryResult.addMetaInfo(metaInfo2);
        QueryTable queryTable1 = new QueryTable(table1);
        ArrayList<Row> rows = new ArrayList<>();
        while (queryTable1.hasNext()) {
            LinkedList<Row> resultRows = new LinkedList<>();
            Row row1 = queryTable1.next();
            QueryTable queryTable2 = new QueryTable(table2);
            resultRows.add(row1);
            while (queryTable2.hasNext()) {
                Row row2 = queryTable2.next();
                Object value1 = row1.getEntries().get(checkPosition1).value;
                Object value2 = row2.getEntries().get(checkPosition2).value;
                if (value1.equals(value2)) {
                    resultRows.add(row2);
                    Row resultRow = queryResult.combineRow(resultRows);
                    rows.add(resultRow);
                }
            }
        }
        return new QueryTable(table1, table2, rows);
    }

    public boolean evalCondition(ConditionItem conditions, Row row, QueryTable queryTable) {
        if (conditions == null) {
            return true;
        }
        boolean a = false;
        if (ConditionItem.class.isInstance(conditions.item_A)) {
            a = evalCondition((ConditionItem) conditions.item_A, row, queryTable);
        } else if (ExpressionItem.class.isInstance(conditions.item_A)) {
            a = evalExpression((ExpressionItem) conditions.item_A, row, queryTable);
        }
        return a;
    }

    public boolean evalExpression(ExpressionItem expression, Row row, QueryTable queryTable) {
        String name = expression.item_A;
        String value = expression.item_B;
        String op = expression.ComparisonOperator;
        Pair<Integer, Column> col = queryTable.getColumn(name);
        int pos = col.getKey();
        ColumnType attrType = col.getValue().type;
        Object leftValue = row.getEntries().get(pos).value;
        if (leftValue == null || value == null){
            return leftValue == null && value == null;
        }
        switch (attrType) {
            case INT:
                Integer left = (Integer) leftValue;
                Integer right = Integer.valueOf(value);
                return expressionCompare(left, right, op);
            case LONG:
                Long lOriginalValue = (Long) leftValue;
                Long lCompareValue = Long.valueOf(value);
                return expressionCompare(lOriginalValue, lCompareValue, op);
            case FLOAT:
                Float fOriginalValue = (Float) leftValue;
                Float fCompareValue = Float.valueOf(value);
                return expressionCompare(fOriginalValue, fCompareValue, op);
            case DOUBLE:
                Double dOriginalValue = (Double) leftValue;
                Double dCompareValue = Double.valueOf(value);
                return expressionCompare(dOriginalValue, dCompareValue, op);
            case STRING:
                return expressionCompare((String) leftValue, value, op);
            default:
                break;
        }
        return false;
    }

    public boolean expressionCompare(Integer l, Integer r, String symbol) {
        switch (symbol) {
            case "=":
                if (l.equals(r))
                    return true;
                break;
            case ">=":
                if (l >= r)
                    return true;
                break;
            case "<=":
                if (l <= r)
                    return true;
                break;
            case ">":
                if (l > r)
                    return true;
                break;
            case "<":
                if (l < r)
                    return true;
                break;
            case "<>":
                if (l != r)
                    return true;
                break;
            default:
                break;
        }
        return false;
    }

    public boolean expressionCompare(Long l, Long r, String symbol) {
        switch (symbol) {
            case "=":
                if (l.equals(r))
                    return true;
                break;
            case ">=":
                if (l >= r)
                    return true;
                break;
            case "<=":
                if (l <= r)
                    return true;
                break;
            case ">":
                if (l > r)
                    return true;
                break;
            case "<":
                if (l < r)
                    return true;
                break;
            case "<>":
                if (l != r)
                    return true;
                break;
            default:
                break;
        }
        return false;
    }

    public boolean expressionCompare(String l, String r, String symbol) {
        switch (symbol) {
            case "=":
                if (l.equals(r))
                    return true;
                break;
            case "<>":
                if (!l.equals(r))
                    return true;
                break;
            default:
                break;
        }
        return false;
    }

    public boolean expressionCompare(Float l, Float r, String symbol) {
        switch (symbol) {
            case "=":
                if (l.equals(r))
                    return true;
                break;
            case ">=":
                if (l >= r)
                    return true;
                break;
            case "<=":
                if (l <= r)
                    return true;
                break;
            case ">":
                if (l > r)
                    return true;
                break;
            case "<":
                if (l < r)
                    return true;
                break;
            case "<>":
                if (l != r)
                    return true;
                break;
            default:
                break;
        }
        return false;
    }

    public boolean expressionCompare(Double l, Double r, String symbol) {
        switch (symbol) {
            case "=":
                if (l.equals(r))
                    return true;
                break;
            case ">=":
                if (l >= r)
                    return true;
                break;
            case "<=":
                if (l <= r)
                    return true;
                break;
            case ">":
                if (l > r)
                    return true;
                break;
            case "<":
                if (l < r)
                    return true;
                break;
            case "<>":
                if (l != r)
                    return true;
                break;
            default:
                break;
        }
        return false;
    }
}
