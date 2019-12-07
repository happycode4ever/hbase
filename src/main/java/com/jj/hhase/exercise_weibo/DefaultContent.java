package com.jj.hhase.exercise_weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DefaultContent {
    private Configuration conf = HBaseConfiguration.create();
    private static final String TABLE_CONTENT = "ns_weibo:content";
    private static final String TABLE_RELATION = "ns_weibo:relation";
    private static final String TABLE_INBOX = "ns_weibo:inbox";

    /*** 发布微博* a、微博内容表中数据+1* b、向微博收件箱表中加入微博的 Rowkey*/
    public void publishContent(String uid, String content) {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            //a、微博内容表中添加 1 条数据，首先获取微博内容表描述
            Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
            // 组装 Rowkey
            long timestamp = System.currentTimeMillis();
            String rowKey = uid + "_" + timestamp;
            //添加微博内容
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), timestamp, Bytes.toBytes(content));
            contentTable.put(put);
            //b、向微博收件箱表中加入发布的 Rowkey//b.1、查询用户关系表，得到当前用户有哪些粉丝
            Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
            //b.2、取出目标数据
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes("fans"));
            Result result = relationTable.get(get);
            List<byte[]> fans = new ArrayList<byte[]>();
            //遍历取出当前发布微博的用户的所有粉丝数据
            for (Cell cell : result.rawCells()) {
                fans.add(CellUtil.cloneQualifier(cell));
            }
            //如果该用户没有粉丝，则直接 return
            if (fans.size() <= 0) return;
            // 开始操作收件箱表
            Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
            //每一个粉丝，都要向收件箱中添加该微博的内容，所以每一个粉丝都是一个 Pu对象
            List<Put> puts = new ArrayList<Put>();
            for (byte[] fan : fans) {
                Put fansPut = new Put(fan);
                fansPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), timestamp,
                        Bytes.toBytes(rowKey));
                puts.add(fansPut);
            }
            inboxTable.put(puts);
        } catch (IOException e) {
        }
    }

    /**
     * 关注用户逻辑
     * a、在微博用户关系表中，对当前主动操作的用户添加新的关注的好友
     * b、在微博用户关系表中，对被关注的用户添加粉丝（当前操作的用户）
     * c、当前操作用户的微博收件箱添加所关注的用户发布的微博 rowkey
     */
    public void addAttends(String uid, String... attends) {
//参数过滤
        if (attends == null || attends.length <= 0 || uid == null || uid.length() <= 0) {
            return;
        }
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
//用户关系表操作对象（连接到用户关系表）
            Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
            List<Put> puts = new ArrayList<Put>();
//a、在微博用户关系表中，添加新关注的好友
            Put attendPut = new Put(Bytes.toBytes(uid));
            for (String attend : attends) {
//为当前用户添加关注的人
                attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend),
                        Bytes.toBytes(attend));
//b、为被关注的人，添加粉丝
                Put fansPut = new Put(Bytes.toBytes(attend));
                fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid),
                        Bytes.toBytes(uid));
//将所有关注的人一个一个的添加到 puts（List）集合中
                puts.add(fansPut);
            }
            puts.add(attendPut);
            relationTable.put(puts);
//c.1、微博收件箱添加关注的用户发布的微博内容（content）的 rowkey
            Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
            Scan scan = new Scan();
//用于存放取出来的关注的人所发布的微博rowkey
            List<byte[]> rowkeys = new ArrayList<byte[]>();
            for (String attend : attends) {//过滤扫描 rowkey，即：前置位匹配被关注的人的 uid_
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new
                        SubstringComparator(attend + "_"));//为扫描对象指定过滤规则
                scan.setFilter(filter);//通过扫描对象得到 scanner
                ResultScanner result = contentTable.getScanner(scan);//迭代器遍历扫描出来的结果集
                Iterator<Result> iterator = result.iterator();
                while (iterator.hasNext()) {
                    //取出每一个符合扫描结果的那一行数据
                    Result r = iterator.next();
                    for (Cell cell : r.rawCells()) {
                        //将得到的 rowkey 放置于集合容器中
                        rowkeys.add(CellUtil.cloneRow(cell));
                    }
                }
            }
            //c.2、将取出的微博 rowkey 放置于当前操作的用户的收件箱
            if (rowkeys.size() <= 0) return;
            //得到微博收件箱表的操作对象
            Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
            //用于存放多个关注的用户的发布的多条微博 rowkey 信息
            List<Put> inboxPutList = new ArrayList<Put>();
            for (byte[] rk : rowkeys) {
                Put put = new Put(Bytes.toBytes(uid));
//uid_timestamp
                String rowKey = Bytes.toString(rk);
//截取 uid
                String attendUID = rowKey.substring(0, rowKey.indexOf("_"));
                long timestamp = Long.parseLong(rowKey.substring(rowKey.indexOf("_") + 1));
//将微博 rowkey 添加到指定单元格中
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendUID), timestamp, rk);
                inboxPutList.add(put);
            }
            inboxTable.put(inboxPutList);
        } catch (
                IOException e) {
            e.printStackTrace();
        } finally {
            if (null != connection) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 取消关注（remove)
     * a、在微博用户关系表中，对当前主动操作的用户删除对应取关的好友
     * b、在微博用户关系表中，对被取消关注的人删除粉丝（当前操作人）
     * c、从收件箱中，删除取关的人的微博的 rowkey
     */
    public void removeAttends(String uid, String... attends) {
//过滤数据
        if (uid == null || uid.length() <= 0 || attends == null || attends.length <= 0) return;
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
//a、在微博用户关系表中，删除已关注的好友
            Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
//待删除的用户关系表中的所有数据
            List<Delete> deleteList = new ArrayList<Delete>();
//当前取关操作者的 uid 对应的 Delete 对象
            Delete attendDelete = new Delete(Bytes.toBytes(uid));
//遍历取关，同时每次取关都要将被取关的人的粉丝-1
            for (String attend : attends) {
                attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));
                //b、在微博用户关系表中，对被取消关注的人删除粉丝（当前操作人）
                Delete fansDelete = new Delete(Bytes.toBytes(attend));
                fansDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
                deleteList.add(fansDelete);
            }
            deleteList.add(attendDelete);
            relationTable.delete(deleteList);
            //c、删除取关的人的微博 rowkey 从 收件箱表中
            Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
            Delete inboxDelete = new Delete(Bytes.toBytes(uid));
            for (String attend : attends) {
                inboxDelete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(attend));
            }
            inboxTable.delete(inboxDelete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取微博实际内容
     * a、从微博收件箱中获取所有关注的人的发布的微博的 rowkey* b、根据得到的 rowkey 去微博内容表中得到数据* c、将得到的数据封装到 Message 对象中
     */
    public List<Message> getAttendsContent(String uid) {
        Connection connection = null;
    try {
        connection = ConnectionFactory.createConnection(conf);
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        //a、从收件箱中取得微博 rowKey
        Get get = new Get(Bytes.toBytes(uid));
        //设置最大版本号
        get.setMaxVersions(5);
        List<byte[]> rowkeys = new ArrayList<byte[]>();
        Result result = inboxTable.get(get);
        for (Cell cell : result.rawCells()) {
            rowkeys.add(CellUtil.cloneValue(cell));
        }
        //b、根据取出的所有 rowkey 去微博内容表中检索数据
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        List<Get> gets = new ArrayList<Get>();
        //根据 rowkey 取出对应微博的具体内容
        for (byte[] rk : rowkeys) {
            Get g = new Get(rk);
            gets.add(g);
        }
        //得到所有的微博内容的 result 对象
        Result[] results = contentTable.get(gets);
        //将每一条微博内容都封装为消息对象
        List<Message> messages = new ArrayList<Message>();
        for (Result res : results) {
            for (Cell cell : res.rawCells()) {
                Message message = new Message();
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String userid = rowKey.substring(0, rowKey.indexOf("_"));
                String timestamp = rowKey.substring(rowKey.indexOf("_") + 1);
                String content = Bytes.toString(CellUtil.cloneValue(cell));
                message.setContent(content);
                message.setTimestamp(timestamp);
                message.setUid(userid);
                messages.add(message);
            }
        }
        return messages;
    } catch(
    IOException e)

    {
        e.printStackTrace();
    }finally

    {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }return null;
}
}
