package com.jj.hhase.client;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class HbaseClient {
    private Admin admin;
    private Connection connection;
    {
        try {
            connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public void dropTable(String tableName) throws IOException {
        if(!tableExists(tableName)){
            System.out.println("当前表"+tableName+"不存在！！");
            return;
        }
        //如果表还是启用状态改为禁用再删除
        if(admin.isTableEnabled(TableName.valueOf(tableName))){
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
        //connection.close();
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param columnFamilys 列族名
     * @throws IOException
     */
    public void createTable(String tableName, String... columnFamilys) throws IOException {
        //旧API构建配置
//        HBaseConfiguration conf = new HBaseConfiguration();
        Configuration conf = HBaseConfiguration.create();
        //旧API获取管理表对象
//        HBaseAdmin admin = new HBaseAdmin(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if(tableExists(tableName)){
            System.out.println("表已存在无法创建");
            return;
        }
        //表描述器
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf : columnFamilys){
            //列描述器
            HColumnDescriptor hcd = new HColumnDescriptor(cf);
            htd.addFamily(hcd);
        }
        //由admin管理表的ddl操作
        admin.createTable(htd);
        admin.close();
        //connection.close();
    }

    /**
     * 表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean tableExists(String tableName) throws IOException{
        Admin admin = ConnectionFactory.createConnection(HBaseConfiguration.create()).getAdmin();
       return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 添加一行数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier
     * @param values
     * @throws IOException
     */
    public void put(String tableName,String rowKey,String columnFamily,String qualifier,String... values) throws IOException {
        if(!tableExists(tableName)){
            System.out.println("当前表"+tableName+"不存在！！");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = Lists.newArrayList();
        for(String value : values){
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            puts.add(put);
        }
        table.put(puts);
        table.close();
        //connection.close();
    }

    /**
     * 删除rowKey整行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public void deleteRow(String tableName,String... rows) throws IOException {
        if(!tableExists(tableName)){
            System.out.println("当前表"+tableName+"不存在！！");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deletes = Lists.newArrayList();
        for(String row : rows){
            deletes.add(new Delete(Bytes.toBytes(row)));
        }
        table.delete(deletes);
        table.close();
        //connection.close();
    }

    /**
     * 删除rowKey部分数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier(可选)
     * @throws IOException
     */
    public void deleteRowWithColumnFamily(String tableName,String rowKey,String columnFamily,String qualifier) throws IOException {
        if(!tableExists(tableName)){
            System.out.println("当前表"+tableName+"不存在！！");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //不指定列删除整个列族
        if(StringUtils.isEmpty(qualifier)){
            delete.addFamily(Bytes.toBytes(columnFamily));
        }else{
            //指定则删除指定列
            delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));
        }
        table.delete(delete);
        table.close();
        //connection.close();
    }

    /**
     * 扫描表输出结果
     * @param tableName
     * @throws IOException
     */
    public void scan(String tableName) throws IOException {
        if(!tableExists(tableName)){
            System.out.println("当前表"+tableName+"不存在！！");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        //可以指定行
//      new Scan(byte[] startRow,byte[] endRow)
        Scan scan = new Scan();
        //可以指定列
//        scan.addColumn(byte[] family,byte[] qualifier)
//        table.getScanner(byte[] family.byte[] qualifier)
        ResultScanner resultScanner = table.getScanner(scan);
        //遍历结果集
        for(Result result : resultScanner){
            //获取每行所有单元格
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //旧API获取数据
//                cell.getRow();
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("row:"+row);
                System.out.println("family:"+family);
                System.out.println("qualifier:"+qualifier);
                System.out.println("value:"+value);
            }
        }
        table.close();
        //connection.close();
    }

    /**
     * 获取部分行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public void printGet(String tableName,String... rows) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Get> gets = Lists.newArrayList();
        for(String row : rows){
            gets.add(new Get(Bytes.toBytes(row)));
        }
        Result[] results = table.get(gets);
        //遍历结果集
        for(Result result : results){
            //获取每行所有单元格
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //旧API获取数据
//                cell.getRow();
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("row:"+row);
                System.out.println("family:"+family);
                System.out.println("qualifier:"+qualifier);
                System.out.println("value:"+value);
            }
        }
        table.close();
        //connection.close();
    }
    /**
     * 获取部分行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public Result[] get(String tableName,String... rows) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Get> gets = Lists.newArrayList();
        for(String row : rows){
            gets.add(new Get(Bytes.toBytes(row)));
        }
        Result[] results = table.get(gets);
        table.close();
        //connection.close();
        return results;
    }

    @Test
    public void test() throws IOException {
//        tableExists("student");
        createTable("fruit_mr","info");
//        dropTable("new");
//        put("person","1003","info","name","keen");
//        put("person","1003","info","sex","female");
//        scan("person");
//        put("person","1003","info","name","mary");
//        deleteRow("person","1002");
//        get("person","1002","1003");
    }

}
