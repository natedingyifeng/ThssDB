package cn.edu.thssdb.schema;

import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableTest {
  private Table table;
  private Column[] columns;

  @Before
  public void setUp() {
    columns=new Column[5];
    columns[0]=new Column("name0", ColumnType.INT,0,true,10);
    columns[1]=new Column("name1", ColumnType.LONG,1,false,10);
    columns[2]=new Column("name2", ColumnType.DOUBLE,0,false,10);
    columns[3]=new Column("name3", ColumnType.FLOAT,0,false,10);
    columns[4]=new Column("name4", ColumnType.STRING,0,false,10);
    table=new Table("db1", "table1", columns);
  }

  @Test
  public void testInsert() {
    try {
      Entry[] entries = {
              new Entry(1),new Entry(new Long(92233712)),new Entry(0.1),new Entry(new Float(2e2)),new Entry("str")
      };
      Row row=new Row(entries);
      table.insert(row);
      assertEquals(table.query(new Entry(new Long(92233712))), row);
    }
    catch (IllegalArgumentException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDelete() {
    try {
      Entry[] entries1 = {
              new Entry(1),new Entry(new Long(92233712)),new Entry(0.1),new Entry(new Float(2e2)),new Entry("str1")
      };
      Row row1=new Row(entries1);
      table.insert(row1);
      table.delete(row1);
      Entry[] entries2 = {
              new Entry(2),new Entry(new Long(92233712)),new Entry(0.2),new Entry(new Float(2e3)),new Entry("str2")
      };
      Row row2=new Row(entries2);
      table.insert(row2);
      assertEquals(table.query(new Entry(new Long(92233712))), row2);
    }
    catch (IllegalArgumentException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testUpdate() {
    try {
      Entry[] entries1 = {
              new Entry(1),new Entry(new Long(92233712)),new Entry(0.1),new Entry(new Float(2e2)),new Entry("str1")
      };
      Entry[] entries2 = {
              new Entry(2),new Entry(new Long(92233712)),new Entry(0.2),new Entry(new Float(2e3)),new Entry("str2")
      };
      Row row1=new Row(entries1);
      Row row2=new Row(entries2);
      table.insert(row1);
      table.update(row2);
      assertEquals(table.query(new Entry(new Long(92233712))), row2);
    }
    catch (IllegalArgumentException e) {
      e.printStackTrace();
    }
  }

  public void printCurrentTable() {
    try {
      Iterator<Row> rowIterator = table.iterator();
      while (rowIterator.hasNext())
      {
        System.out.println(rowIterator.next().getEntries().toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void printDataFile() {
    try {
      FileInputStream deserializeStream = new FileInputStream(table.databaseFolderPath + table.tableName + ".data");
      ObjectInputStream inStream = new ObjectInputStream(deserializeStream);
      while (true) {
        try {
          Row singleRow = (Row) inStream.readObject();
          System.out.println(singleRow.getEntries().toString());
        } catch (Exception e) {
          break;
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPersistAndRecover() {
    try {
      // Table Initial
      Entry[] entries1 = {
              new Entry(1),new Entry(new Long(92233712)),new Entry(0.1),new Entry(new Float(2e2)),new Entry("str1")
      };
      Entry[] entries2 = {
              new Entry(2),new Entry(new Long(92233711)),new Entry(0.2),new Entry(new Float(2e3)),new Entry("str2")
      };
      Row row1=new Row(entries1);
      Row row2=new Row(entries2);
      table.insert(row1);
      table.insert(row2);
      System.out.println("Current Table: ");
      printCurrentTable();

      // Persist
      table.persist();
      System.out.println("Data Persist: ");
      printDataFile();

      // Table Update
      Entry[] entries3 = {
              new Entry(3),new Entry(new Long(9223372)),new Entry(0.3),new Entry(new Float(2e4)),new Entry("str3")
      };
      Entry[] entries4 = {
              new Entry(4),new Entry(new Long(9223711)),new Entry(0.4),new Entry(new Float(2e5)),new Entry("str4")
      };
      Row row3=new Row(entries3);
      Row row4=new Row(entries4);
      table.insert(row3);
      table.insert(row4);
      System.out.println("Before Recover: ");
      printCurrentTable();

      // Table Recover
      table.recover();
      System.out.println("After Recover: ");
      printCurrentTable();
    }
    catch (IllegalArgumentException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPageFilePersistAndRecover() {
    // TODO
  }

//  @After
//  public void setDown() {
//
//  }
}
