package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class MetaTest {
    Manager manager = Manager.getInstance();
    Column[] columns;

    @Before
    public void setUp(){
//        manager = Manager.getInstance();

        columns=new Column[5];
        columns[0]=new Column("name0", ColumnType.INT,0,true,10);
        columns[1]=new Column("name1", ColumnType.LONG,1,false,10);
        columns[2]=new Column("name2", ColumnType.DOUBLE,0,false,10);
        columns[3]=new Column("name3", ColumnType.FLOAT,0,false,10);
        columns[4]=new Column("name4", ColumnType.STRING,0,false,10);
    }

    /**
     * 进行了数据库的创建，删除，切换的测试。
     * 进行测试前先删除data内相应数据库的文件
     */
    @Test
    public void databaseTest(){
        // 数据库的创建
        manager.transactionOfCreateDatabase("testdb1");
        manager.transactionOfCreateDatabase("testdb2");
        assertTrue(manager.contain("testdb1"));
        assertTrue(manager.contain("testdb2"));

        // 数据库的切换
        manager.switchDatabase("testdb1");
        assertTrue(manager.databaseOnline("testdb1"));
        manager.quitDatabase("testdb1");
        manager.switchDatabase("testdb2");
        assertFalse(manager.databaseOnline("testdb1"));
        assertTrue(manager.databaseOnline("testdb2"));

        // 数据库的删除
        manager.deleteDatabase("testdb2");
        assertFalse(manager.contain("testdb2"));
        assertFalse(manager.databaseOnline("testdb2"));
    }

    /**
     * 进行表创建和删除的测试，查看是否有相应的元数据生成。
     * 请手动查看相应元数据文件
     */
    @Test
    public void metaTest(){
        manager.switchDatabase("testdb1");
        Database db1 = manager.getDatabase("testdb1");
        db1.create("table1", columns);
        db1.create("table2", columns);
        assertTrue(db1.containTable("table1"));
        assertTrue(db1.containTable("table2"));

        db1.create("table3",columns);
        db1.drop("table3");
        assertFalse(db1.containTable("table3"));
        manager.getDatabase("testdb1").quit();
    }

    /**
     * 重启数据库，测试恢复的系统信息是否正确。
     * 请单独运行
     */
    @Test
    public void recoverTest(){
        // 测试testdb1和testdb2是否恢复
        assertTrue(manager.contain("testdb1"));
        assertFalse(manager.contain("testdb2"));

        // 测试testdb1中的表是否恢复
        manager.switchDatabase("testdb1");
        Database db1 = manager.getDatabase("testdb1");
        assertTrue(db1.containTable("table1"));
        assertTrue(db1.containTable("table2"));
        assertFalse(db1.containTable("table3"));
        for(int i = 0; i < columns.length; i++){
            assertEquals(db1.getTable("table1").columns.get(i).toString(), columns[i].toString());
        }


    }

}
