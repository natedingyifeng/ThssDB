package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;


@FixMethodOrder(MethodSorters.DEFAULT)
public class QueryTest {
    private ThssParser parser = new ThssParser();;
    private String testString;

    @Before
    public void setUp() {
        testString = "CREATE DATABASE testdb1";
        parser.parseThss_stmts(testString);
    }

    @Test
    public void testCreate(){
        testString = "CREATE TABLE table1 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
    }

    @Test
    public void testDrop(){
        testString = "CREATE TABLE table2 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
        testString = "DROP TABLE table2";
        parser.parseThss_stmts(testString);
    }

    @Test
    public void testShow(){
        testString = "CREATE TABLE table3 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
        testString = "SHOW TABLE table3";
        parser.parseThss_stmts(testString);
    }

    @Test
    public void testInsert(){
        testString = "CREATE TABLE table4 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table4 VALUES ('str1', 1)";
        parser.parseThss_stmts(testString);
    }

    @Test
    public void testDelete(){
        testString = "CREATE TABLE table5 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table5 VALUES ('str2', 2)";
        parser.parseThss_stmts(testString);
        testString = "DELETE FROM table5 WHERE name0 = 'str2'";
        parser.parseThss_stmts(testString);
    }

    @Test
    public void testUpdate(){
        testString = "CREATE TABLE table7 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table7 VALUES ('str2', 2)";
        parser.parseThss_stmts(testString);
        testString = "UPDATE table7 SET name0 = 'str3' WHERE name1 = 2";
        parser.parseThss_stmts(testString);
    }

    @Test
    public void testSelect(){
        testString = "CREATE TABLE table8 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
        testString = "CREATE TABLE table9 (name0 String(32), name1 Int not null, PRIMARY KEY(name1))";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table8 VALUES ('str1', 1)";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table8 VALUES ('str2', 2)";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table8 VALUES ('str3', 3)";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table9 VALUES ('str3', 3)";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table9 VALUES ('str1', 1)";
        parser.parseThss_stmts(testString);
        testString = "INSERT INTO table9 VALUES ('str2', 2)";
        parser.parseThss_stmts(testString);
        testString = "SELECT * FROM table8";
        parser.parseThss_stmts(testString);
        testString = "SELECT name0 FROM table8";
        parser.parseThss_stmts(testString);
        testString = "SELECT name0, name1 FROM table8 WHERE name1 <> 2";
        parser.parseThss_stmts(testString);
        testString = "SELECT name1 FROM table8 WHERE name1 <> 2";
        parser.parseThss_stmts(testString);
        testString = "SELECT table8.name1, table9.name1 FROM table8 JOIN table9 ON table8.name0 = table9.name0 WHERE name1 <> 2";
        parser.parseThss_stmts(testString);
        testString = "SELECT person.ID, score.score FROM person JOIN score ON person.name = score.name";
        parser.parseThss_stmts(testString);
        testString = "SELECT * FROM person JOIN score ON person.name = score.name ORDER BY score";
        parser.parseThss_stmts(testString);
        testString = "SELECT person.ID, score.score FROM person JOIN score ON person.name = score.name WHERE score >= 90 OR score <= 85";
        parser.parseThss_stmts(testString);
        testString = "BEGIN TRANSACTION";
        parser.parseThss_stmts(testString);

        testString = "ROLLBACK";
        parser.parseThss_stmts(testString);

        testString = "ROLLBACK TO A";
        parser.parseThss_stmts(testString);

        testString = "SAVEPOINT A";
        parser.parseThss_stmts(testString);

        testString = "COMMIT";
        parser.parseThss_stmts(testString);
    }
}
