package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.tableException.*;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.base.ConditionItem;
import cn.edu.thssdb.query.base.ExpressionItem;
import cn.edu.thssdb.query.base.FromItem;
import cn.edu.thssdb.transaction.LogData;
import cn.edu.thssdb.utils.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Operations {
    private String database_name = "test";
    public Database database;
    private String result;
    public Manager manager;
    public ComplexDataOperation compare = new ComplexDataOperation();

    public Operations() {
    }

    public void createDatabase(String name) {
        this.database_name = name;
        manager.transactionOfCreateDatabase(database_name);
        this.database = manager.databases.get(database_name);
        compare = new ComplexDataOperation();
    }

    public Object createTable(String table_name, ArrayList<Column> column) {
        database.create(table_name, column);
        return database.printTable(table_name);
    }

    public void dropTable(String table_name) {
        database.drop(table_name);
    }

    public Object showTable(String table_name) {
        result = database.show_info(table_name);
        System.out.print(result);
        return result;
    }

    public LogData insertTable(String table_name, Row entry) {
        table_name = table_name.toUpperCase();
        if (database.tables.containsKey(table_name)) {
            Table table = database.tables.get(table_name);
            table.insert(entry);
        } else {
            throw new TableNotExistException();
        }
//        database.persist();
        return new LogData(LogData.LogStatType.INSERT, table_name, entry);
    }

    public ArrayList<LogData> deleteTable(String table_name, ConditionItem entry) {
        ArrayList<LogData> logDatas = new ArrayList<>();
        if (database.tables.containsKey(table_name)) {
            Table table = database.tables.get(table_name);
            QueryTable queryTable = new QueryTable(table);
            ArrayList<Row> rowList = new ArrayList<>();
            while (queryTable.hasNext()) {
                Row row = queryTable.next();
                if (compare.evalCondition(entry, row, queryTable)) {
                    rowList.add(row);
                }
            }
            for (Row row : rowList) {
                logDatas.add(new LogData(LogData.LogStatType.DELETE, table_name, row));
                table.delete(row);
            }
        } else {
            throw new TableNotExistException();
        }
//        database.persist();
        return logDatas;
    }

    public ArrayList<LogData> updateTable(String table_name, String column_name, Entry column_expression, ConditionItem multiple_conditions) {
        table_name = table_name.toUpperCase();
        ArrayList<LogData> logDatas = new ArrayList<>();
        if (database.tables.containsKey(table_name)) {
            Table table = database.tables.get(table_name);
            QueryTable queryTable = new QueryTable(table);
            ArrayList<Row> rowList = new ArrayList<>();
            while (queryTable.hasNext()) {
                Row row = queryTable.next();
                if (compare.evalCondition(multiple_conditions, row, queryTable)) {
                    rowList.add(row);
                }
            }
            int position = queryTable.getColumn(column_name).getKey();
            for (Row row : rowList) {
                Entry oldVal = row.getEntries().get(position);
                row.updateEntery(position, column_expression);
                logDatas.add(new LogData(LogData.LogStatType.UPDATE, table_name, row, oldVal, column_expression, position));
                table.update(row);
            }
        } else
            throw new TableNotExistException();
//        database.persist();
        database.printTable(table_name);
        return logDatas;
    }

    public Object selectTable(ArrayList<String> result_column, FromItem table_query, ConditionItem multiple_conditions) {
        Map<String, Object> map = new HashMap<>();
        ArrayList<ArrayList<String>> rowsList = new ArrayList<>();
        ArrayList<String> columnList = new ArrayList<>();
        ArrayList<Row> rowList = new ArrayList<>();
        QueryResult queryResult = new QueryResult();;
        Table table;
        Table table_2;
        String table_name;
        String table_name_2;
        QueryTable queryTable;
        if (table_query.table_name.size() == 1) {
            table_name = table_query.table_name.get(0);
            if (database.tables.containsKey(table_name)) {
                table = database.tables.get(table_name);
                MetaInfo metaInfo = new MetaInfo(table_name, table.columns);
                queryTable = new QueryTable(table);
                queryResult = new QueryResult();
                queryResult.addMetaInfo(metaInfo);
                for (String name : result_column) {
                    if (name.equals("*")) {
                        for (Column column : table.columns) {
                            queryResult.addIndex(table_name, column.name);
                            columnList.add(column.name);
                        }
                    } else {
                        queryResult.addIndex(table_name, name);
                        columnList.add(name);
                    }
                }
                while (queryTable.hasNext()) {
                    Row row = queryTable.next();
                    if (compare.evalCondition(multiple_conditions, row, queryTable)) {
                        Row r = queryResult.generateQueryRecord(row);
                        rowList.add(r);
                    }
                }
            }
            else {
                throw new TableNotExistException();
            }
        } else if (table_query.table_name.size() == 2) {
            table_name = table_query.table_name.get(0);
            table_name_2 = table_query.table_name.get(1);
            if (database.tables.containsKey(table_name) && database.tables.containsKey(table_name_2)) {
                table = database.tables.get(table_name);
                table_2 = database.tables.get(table_name_2);
                queryTable = compare.getJoinRow(table, table_2, (ExpressionItem) table_query.condition.item_A);
                MetaInfo metaInfo = new MetaInfo(table.tableName, table.columns);
                MetaInfo metaInfo_2 = new MetaInfo(table_2.tableName, table_2.columns);
                queryResult.addMetaInfo(metaInfo);
                queryResult.addMetaInfo(metaInfo_2);

                for (String name : result_column) {
                    if (name.equals("*")) {
                        for (Column column : table.columns) {
                            queryResult.addIndex(table.tableName, column.name);
                            columnList.add(column.name);
                        }
                        for (Column column : table.columns) {
                            queryResult.addIndex(table_2.tableName, column.name);
                            columnList.add(column.name);
                        }
                    } else {
                        String[] attrs = name.split("\\.");
                        if (attrs.length > 1) {
                            queryResult.addIndex(attrs[0], attrs[1]);
                            columnList.add(name);
                        } else {
                            if (table.examineAttr(attrs[0])) {
                                queryResult.addIndex(table_name, attrs[0]);
                            } else {
                                if (table_2.examineAttr(attrs[0])) {
                                    queryResult.addIndex(table_name_2, attrs[0]);
                                }
                            }
                        }
                    }
                }

                while (queryTable.hasNext()) {
                    Row row = queryTable.next();
                    if (compare.evalCondition(multiple_conditions, row, queryTable)) {
                        Row r = queryResult.generateQueryRecord(row);
                        rowList.add(r);
                    }
                }
            } else {
                throw new KeyNotExistException();
            }
        }
        for (Row r : rowList) {
            ArrayList<String> row = new ArrayList<>();
            for (Entry e : r.entries) {
                row.add(e.toString());
            }
            rowsList.add(row);
        }
        map.put("columnsList", columnList);
        map.put("rowList", rowsList);
        return map;
    }
}
