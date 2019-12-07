package com.jj.hhase.exercise_weibo;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class WeiboTest {
    @Test
    public void test() throws IOException {
        ContentManagement cm = new ContentManagement();
//        cm.publishContent("1001","我的微博10011111");
//        cm.publishContent("1001","我的微博10012222");
//        cm.publishContent("1002","我的微博10021111");
//        cm.publishContent("1002","我的微博10022222");
//        cm.publishContent("1002","我的微博10023333");
//        cm.publishContent("1003","我的微博10031111");
//        cm.publishContent("1003","我的微博10032222");
//        cm.addAttends("1003","1001","1002");
        cm.cancelAttends("1003","1002");
        cm.showContent("1003");
    }
    @Test
    public void test2() {
        DefaultContent dc = new DefaultContent();
//        dc.publishContent("1001","我的微博10011111");
//        dc.publishContent("1001","我的微博10012222");
//        dc.publishContent("1002","我的微博10021111");
//        dc.publishContent("1002","我的微博10022222");
//        dc.publishContent("1002","我的微博10023333");
//        dc.publishContent("1003","我的微博10031111");
//        dc.publishContent("1003","我的微博10032222");
        dc.addAttends("1003","1002","1001");
//        dc.removeAttends("1003","1002");
        List<Message> messages = dc.getAttendsContent("1003");
        messages.stream().forEach(m->{
            System.out.println(m);
        });

    }

    @Test
    public void testDelete1() throws IOException {
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        Table inboxTable = connection.getTable(TableName.valueOf(InitTable.INBOX_TABLE));
        Delete delete = new Delete(Bytes.toBytes("1003"));
        delete.addColumns(Bytes.toBytes("info"),Bytes.toBytes("1001"));
        inboxTable.delete(delete);
        inboxTable.close();
    }
    @Test
    public void testDelete2() throws IOException {
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        Table relationTable = connection.getTable(TableName.valueOf(InitTable.RELATION_TABLE));
        Delete delete = new Delete(Bytes.toBytes("1003"));
        delete.addColumn(Bytes.toBytes("attends"),Bytes.toBytes("1002"));
        relationTable.delete(delete);
        relationTable.close();
    }

    @Test
    public void testPut2() throws IOException {
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        Table table = connection.getTable(TableName.valueOf(InitTable.RELATION_TABLE));
        Put put = new Put(Bytes.toBytes("1003"));
        put.addColumn(Bytes.toBytes("attends"),Bytes.toBytes("1001"),Bytes.toBytes(""));
        table.put(put);
        table.close();

    }
    @Test
    public void testPut(){
        Put put = new Put(Bytes.toBytes("myrow"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column1"),Bytes.toBytes("value1"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column1"),Bytes.toBytes("value11"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column2"),Bytes.toBytes("value2"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column3"),Bytes.toBytes("value3"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column4"),Bytes.toBytes("value4"));
        put.get(Bytes.toBytes("cf"),Bytes.toBytes("column1")).stream().forEach(cell->{
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        });
//        put.getFamilyCellMap().values().stream().forEach(list->list.stream().forEach(cell-> {
//            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
//            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
//            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
//            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
//        }));
    }
}
