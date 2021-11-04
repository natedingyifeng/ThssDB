package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.SQLBaseVisitor;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.base.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Operations;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.GsonUtil;
import cn.edu.thssdb.utils.Pair;
import com.google.gson.reflect.TypeToken;
import org.antlr.v4.runtime.tree.ParseTree;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;

public class ThssVisitor extends SQLBaseVisitor<Object> {
//    public Operations operation = new Operations();

    @Override
    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<Statement> stmts = new ArrayList<>();
        for(int i=0;i<ctx.getChildCount();i++){
            Statement stmt = (Statement) visit(ctx.getChild(i));
            if(stmt!=null) {
                stmts.add(stmt);
            }
        }
        return stmts;
    }

    @Override
    public Object visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        return visit(ctx.getChild(0));
    }

    @Override
    public Object visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String database_name = (String) visit(ctx.getChild(2));
        Object[] params = {database_name};
        Statement create_db_stmt = new Statement(Global.STMT_TYPE.CREATE_DATABASE, params);
//        operation.createDatabase(database_name);
        return create_db_stmt;
    }

    @Override
    public Object visitAlter_table_stmt(SQLParser.Alter_table_stmtContext ctx) {
        String table_name = (String) visit(ctx.getChild(2));
        Column column_def = null;
        boolean isAdd = false;
        String column_name = null;
        if (ctx.getChild(3).getText().equalsIgnoreCase("ADD")) {
            column_def = (Column) visit(ctx.getChild(4));
            isAdd=true;
        }
        else if (ctx.getChild(3).getText().equalsIgnoreCase("DROP")) {
            column_name = (String) visit(ctx.getChild(4));
            isAdd=false;
        }
        Object[] params = {table_name, column_def, column_name,isAdd};
        return new Statement(Global.STMT_TYPE.ALTER_TABLE, params);
    }

    @Override
    public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String table_name = (String) visit(ctx.getChild(2));
        ArrayList<Column> columns = new ArrayList<>();
        HashSet<String> primary_keys = new HashSet<>();
        for (int i=4;i<ctx.getChildCount();i++)
        {
            if (SQLParser.Column_defContext.class.isInstance(ctx.getChild(i))){
                columns.add((Column)visit(ctx.getChild(i)));
            }
            else if (SQLParser.Table_constraintContext.class.isInstance(ctx.getChild(i))){
                primary_keys = (HashSet<String>) visit(ctx.getChild(i));
            }
        }
        for(Column column : columns){
            if (primary_keys.contains(column.getName())) {
                column.setPrimaryKey();
            }
        }
        Object[] params = {table_name, columns};
        Column[] columns_param = new Column[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columns_param[i] = columns.get(i);
        }
//        operation.createTable(table_name, columns);
        return new Statement(Global.STMT_TYPE.CREATE_TABLE, params);
    }

    @Override
    public Object visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String table_name = (String) visit(ctx.children.get(2));
        Object[] params = {table_name};
//        operation.showTable(table_name);
        return new Statement(Global.STMT_TYPE.SHOW_TABLE, params);
    }

    @Override
    public Object visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String table_name = null;
        Object[] params;
        table_name = (String) visit(ctx.getChild(2));
        if(ctx.getChildCount() == 3) {
            params = new Object[]{table_name, null};
//            operation.deleteTable(table_name, null);
            return new Statement(Global.STMT_TYPE.DELETE, params);
        }
        else {
            ConditionItem condition_item = (ConditionItem) visit(ctx.getChild(4));
            params = new Object[]{table_name, condition_item};
//            operation.deleteTable(table_name, condition_item);
            return new Statement(Global.STMT_TYPE.DELETE, params);
        }
    }

    @Override
    public Object visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String table_name = (String) visit(ctx.getChild(2));
        Object[] params = {table_name};
//        operation.dropTable(table_name);
        return new Statement(Global.STMT_TYPE.DROP_TABLE, params);
    }

    @Override
    public Object visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String table_name = (String) visit(ctx.getChild(2));
//        Entry entry_value = null;
//        Row row=null;
//        for(int i=3;i<ctx.getChildCount();i++) {
//            if (SQLParser.Value_entryContext.class.isInstance(ctx.getChild(i))){
////                entry_value = (Entry) visit(ctx.getChild(i));
//                row = (Row)visit(ctx.getChild(i));
//            }
//        }
////        System.out.println(entry_value.toString());
////        Entry[] entries = {new Entry("Bob"),new Entry(15)};
////        Object[] params = {table_name, entry_value};
//        Object[] params = {table_name, row};
////        operation.insertTable(table_name, entries);
        Row row_value = null;
        for(int i=3;i<ctx.getChildCount();i++) {
            if (SQLParser.Value_entryContext.class.isInstance(ctx.getChild(i))){
                row_value = (Row) visit(ctx.getChild(i));
            }
        }
        Object[] params = {table_name, row_value};
//        operation.insertTable(table_name,row_value);
        return new Statement(Global.STMT_TYPE.INSERT, params);
    }



    @Override
    public Object visitValue_entry(SQLParser.Value_entryContext ctx) {
//        System.out.println(ctx.getText());
//        return new Entry(ctx.getText());
        Row row = new Row();
        for(int i=0; i< ctx.getChildCount(); i++){
            if(SQLParser.Literal_valueContext.class.isInstance(ctx.getChild(i))){
                row.appendEntry((Entry) visit(ctx.getChild(i)));
            }
        }
        return row;
    }

    @Override
    public Object visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        ArrayList<String> result_column = new ArrayList<>();
        FromItem table_query = null;
        ConditionItem multiple_conditions = null;
        for (ParseTree node : ctx.children){
            if (SQLParser.Result_columnContext.class.isInstance(node)){
                result_column.add((String)visit(node));
            }
            else if (SQLParser.Table_queryContext.class.isInstance(node)){
                table_query = (FromItem) visit(node);
            }
            else if (SQLParser.Multiple_conditionContext.class.isInstance(node)){
                multiple_conditions = (ConditionItem) visit(node);
            }
        }
//        operation.select(result_column, table_query, multiple_conditions);
        Object[] params = {result_column, table_query, multiple_conditions};
        return new Statement(Global.STMT_TYPE.SELECT, params);
    }

    @Override
    public Object visitTable_query(SQLParser.Table_queryContext ctx) {
        ArrayList<String> tableNames = new ArrayList<>();
        ConditionItem conditions = null;
        for(ParseTree node:ctx.children){
            if (SQLParser.Table_nameContext.class.isInstance(node)){
                String tableName = (String)visit(node);
                tableNames.add(tableName);
            }
            else if (SQLParser.Multiple_conditionContext.class.isInstance(node)){
                conditions = (ConditionItem) visit(node);
            }

        }
        return new FromItem(tableNames, conditions);
    }

    @Override
    public Object visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String table_name = (String) visit(ctx.getChild(1));
        String column_name = (String) visit(ctx.getChild(3));
        Entry column_expression = (Entry) visit(ctx.getChild(5));
        Object[] params;
        if (ctx.getChildCount() > 6)
        {
            ConditionItem multiple_conditions = (ConditionItem) visit(ctx.getChild(7));
//            operation.updateTable(table_name, column_name, column_expression, multiple_conditions);
            params = new Object[]{table_name, column_name, column_expression, multiple_conditions};
            return new Statement(Global.STMT_TYPE.UPDATE, params);
        }
        else {
//            operation.updateTable(table_name, column_name, column_expression, null);
            params = new Object[]{table_name, column_name, column_expression, null};
            return new Statement(Global.STMT_TYPE.UPDATE, params);
        }
    }

    @Override
    public Object visitBegin_transaction_stmt(SQLParser.Begin_transaction_stmtContext ctx) {
        return new Statement(Global.STMT_TYPE.BEGIN_TRANSACTION, null);
    }

    @Override
    public Object visitCommit_stmt(SQLParser.Commit_stmtContext ctx) {
        return new Statement(Global.STMT_TYPE.COMMIT, null);
    }

    @Override public Object visitRollback_stmt(SQLParser.Rollback_stmtContext ctx) {
        if (ctx.children.size() == 1)
        {
            return new Statement(Global.STMT_TYPE.ROLLBACK, null);
        }
        else if (ctx.children.size() == 3){
            String rollback_point = ctx.getChild(2).getText().toUpperCase();
            Object[] params = {rollback_point};
            return new Statement(Global.STMT_TYPE.ROLLBACK, params);
        }
        else return null;
    }

    @Override public Object visitSavepoint_stmt(SQLParser.Savepoint_stmtContext ctx) {
        String savepoint = ctx.getChild(1).getText().toUpperCase();
        Object[] params = {savepoint};
        return new Statement(Global.STMT_TYPE.SAVEPOINT, params);
    }

    @Override
    public Object visitColumn_def(SQLParser.Column_defContext ctx) {
        String column_name = (String) visit(ctx.getChild(0));
        Pair<ColumnType, Integer> column_type = (Pair<ColumnType, Integer>) visit(ctx.getChild(1));
        boolean not_null = false;
        int is_primary = 0;
        if (ctx.getChildCount() > 2)
        {
            for(int i=2; i< ctx.getChildCount(); i++){
                if(ctx.getChild(i).getText().toUpperCase().equals("KEY")){
                    is_primary = 1;
                }
                if(ctx.getChild(i).getText().toUpperCase().equals("NULL")) {
                    not_null = true;
                }
            }
        }
        return new Column(column_name, column_type.left, is_primary, not_null, column_type.right);
    }

    @Override
    public Object visitType_name(SQLParser.Type_nameContext ctx) {
        String type_name = ctx.getChild(0).getText().toUpperCase();
        ColumnType column_type = null;
        int string_length = 0;
        if(type_name.equals("INT"))
        {
            column_type = ColumnType.INT;
        }
        else if (type_name.equals("LONG"))
        {
            column_type = ColumnType.LONG;
        }
        else if (type_name.equals("FLOAT"))
        {
            column_type = ColumnType.FLOAT;
        }
        else if (type_name.equals("DOUBLE"))
        {
            column_type = ColumnType.DOUBLE;
        }else if (type_name.equals("STRING"))
        {
            column_type = ColumnType.STRING;
            if (ctx.getChildCount() == 4)
            {
                string_length = Integer.parseInt(ctx.getChild(2).getText());
            }
        }
        return new Pair<>(column_type, string_length);
    }

    @Override
    public Object visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        Object multiple_condition = null;
        if (ctx.getChildCount() == 1) {
            multiple_condition = new ConditionItem(visit(ctx.getChild(0)));
        }
        else if (ctx.getChildCount() == 3) {
            multiple_condition = new ConditionItem((Object) visit(ctx.getChild(0)), (Object) visit(ctx.getChild(2)), ctx.getChild(1).getText());
        }
        return multiple_condition;
    }

    @Override
    public Object visitCondition(SQLParser.ConditionContext ctx) {
        ExpressionItem condition = null;
        if (ctx.getChildCount() == 1) {
            condition = new ExpressionItem((String)visit(ctx.getChild(0)), null, "");
        }
        else if (ctx.getChildCount() == 3) {
            if (Entry.class.isInstance(visit(ctx.getChild(2)))){
                condition = new ExpressionItem((String)visit(ctx.getChild(0)), ((Entry)visit(ctx.getChild(2))).value.toString(), ctx.getChild(1).getText());
            }
            else {
                condition = new ExpressionItem((String)visit(ctx.getChild(0)), (String)(visit(ctx.getChild(2))), ctx.getChild(1).getText());
            }
        }
        return condition;
    }

    @Override
    public Object visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        HashSet<String> primary_keys = new HashSet<>();
        for(int i=0;i<ctx.getChildCount();i++){
            if (SQLParser.Column_nameContext.class.isInstance(ctx.getChild(i))){
                primary_keys.add((String)visit(ctx.getChild(i)));
            }
        }
        return primary_keys;
    }

    @Override
    public Object visitResult_column(SQLParser.Result_columnContext ctx) {
        return ctx.getText().toUpperCase();
    }

    @Override
    public Object visitLiteral_value(SQLParser.Literal_valueContext ctx) {
        if (ctx.getText().equalsIgnoreCase("NULL")){
            return new Entry(null);
        }
        else{
            Type type = new TypeToken<Object>() {}.getType();
            Object obj = GsonUtil.fromJson(ctx.getText(), type);
            if(obj.toString().split("\\.").length!=1&&ctx.getText().toString().split("\\.").length==1)
            {
                //更正fromJson将int转为double问题
                obj=Integer.parseInt(ctx.getText());
            }
            return new Entry((Comparable) obj);
//            return new Entry(ctx.getText());
        }
    }

    @Override
    public Object visitDatabase_name(SQLParser.Database_nameContext ctx) {
        return ctx.getChild(0).getText().toUpperCase();
    }

    @Override
    public Object visitTable_name(SQLParser.Table_nameContext ctx) {
        return ctx.getChild(0).getText().toUpperCase();
    }

    @Override
    public Object visitColumn_name(SQLParser.Column_nameContext ctx) {
        return ctx.getChild(0).getText().toUpperCase();
    }

    @Override
    public Object visitColumn_full_name(SQLParser.Column_full_nameContext ctx) {
        return ctx.getText().toUpperCase();
    }

    @Override public Object visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        String databaseName = null;
        for(ParseTree node:ctx.children) {
            if (SQLParser.Database_nameContext.class.isInstance(node)) {
                databaseName = (String) visit(node);
            }
        }
        Object[] params = {databaseName};
        return new Statement(Global.STMT_TYPE.USE, params);
    }
}
