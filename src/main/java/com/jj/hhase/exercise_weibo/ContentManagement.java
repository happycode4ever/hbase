package com.jj.hhase.exercise_weibo;

import com.google.common.collect.Lists;
import com.jj.hhase.client.HbaseClient;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ContentManagement {
    private Configuration conf;
    private Connection connection;
    private Table contentTable;
    private Table relationTable;
    private Table inboxTable;
    private HbaseClient client;

    {
        try {
            client = new HbaseClient();
            conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            inboxTable = connection.getTable(TableName.valueOf(InitTable.INBOX_TABLE));
            contentTable = connection.getTable(TableName.valueOf(InitTable.CONTENT_TABLE));
            relationTable = connection.getTable(TableName.valueOf(InitTable.RELATION_TABLE));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 发布微博
     * 内容表新增记录,row是uid_timestamp
     * 查找当前用户的粉丝
     * 收件箱表推这条微博给所有粉丝
     *
     * @param uid
     * @param content
     */
    public void publishContent(String uid, String... content) throws IOException {
        //内容表新增记录
        long currentTime = System.currentTimeMillis();
        String rowKey = uid + "_" + currentTime;
        client.put(InitTable.CONTENT_TABLE, rowKey, "info", "content", content);
        //根据uid查询关系表的粉丝列族所有列添加到结果集
        Get relationGet = new Get(Bytes.toBytes(uid));
        relationGet.addFamily(Bytes.toBytes("fans"));
        Result result = relationTable.get(relationGet);
        List<String> fansUids = Lists.newArrayList();
        for (Cell cell : result.rawCells()) {
            fansUids.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
        }
        //没粉丝不推微博
        if (CollectionUtils.isNotEmpty(fansUids)) {
            //构建收件箱puts
            List<Put> inboxPuts = Lists.newArrayList();
            for (String fanUid : fansUids) {
                //rowKey是当前用户
                Put inboxPut = new Put(Bytes.toBytes(fanUid));
                //列是粉丝id，时间戳取自发送时间戳
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), currentTime, Bytes.toBytes(rowKey));
                inboxPuts.add(inboxPut);
            }
            inboxTable.put(inboxPuts);
        }
        inboxTable.close();
        relationTable.close();
        //connection.close();
    }

    /**
     * 关注
     * 关系表为当前用户添加关注列族
     * 被关注用户的粉丝列族新增当前用户
     * 新关注用户要推到当前用户的收件箱中
     *
     * @param uid
     * @param attendUids
     */
    public void addAttends(String uid, String... attendUids) throws IOException {
        List<Put> relationPuts = Lists.newArrayList();
        Put attendPut = new Put(Bytes.toBytes(uid));
        for (String attendUid : attendUids) {
            //添加关注
            attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attendUid), Bytes.toBytes(""));
            //被关注的粉丝添加我
            Put fanPut = new Put(Bytes.toBytes(attendUid));
            fanPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(""));
            relationPuts.add(fanPut);
        }
        relationPuts.add(attendPut);
        relationTable.put(relationPuts);

        List<Put> inboxPuts = Lists.newArrayList();
        //内容表提关注的uid
        Scan contentScan = new Scan();
        for (String attendUid : attendUids) {
            //设置过滤规则，取rokKey子串等于关注id+_
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attendUid + "_"));
            contentScan.setFilter(rowFilter);
            //设置最新五个版本
            contentScan.setMaxVersions(5);
            ResultScanner resultScanner = contentTable.getScanner(contentScan);
            //当前关注人的最新五条微博rowKey推到收件箱,注意timestamp要用发送微博的时间戳
            for (Result result : resultScanner) {
                Put inboxPut = new Put(Bytes.toBytes(uid));
                byte[] row = result.getRow();
                //微博内容表的key拆分时间戳
                long timestamp = Long.valueOf(Bytes.toString(row).split("_")[1]);
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendUid), timestamp, row);
                inboxPuts.add(inboxPut);
            }
        }
        inboxTable.put(inboxPuts);
        inboxTable.close();
        relationTable.close();
        contentTable.close();
        //connection.close();
    }

    /**
     * 取消关注
     * 当前用户关注列族清理
     * 被关注用户粉丝列族清理我
     * 我的收件箱清理关注的用户列
     */
    public void cancelAttends(String uid, String... attendUids) throws IOException {
        List<Delete> relationDeletes = Lists.newArrayList();
        //删除我的关注只涉及一行
        Delete attendDelete = new Delete(Bytes.toBytes(uid));
        for (String attendUid : attendUids) {
            //我的关注列族包含多个列删除
            attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attendUid));
            Delete fanDelete = new Delete(Bytes.toBytes(attendUid));
            //关注用户的粉丝列族删除我
            fanDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            relationDeletes.add(fanDelete);
        }
        relationDeletes.add(attendDelete);
        relationTable.delete(relationDeletes);

        //收件箱表删除列中的关注
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for (String attendUid : attendUids) {
            inboxDelete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(attendUid));
        }
        inboxTable.delete(inboxDelete);
        inboxTable.close();
        relationTable.close();
        //connection.close();
    }

    /**
     * 删除微博
     * 删除自己的微博
     * 粉丝的收件箱清理自己
     */
    public void deleteContent(String uid, String rowKey) throws IOException {
        //获取粉丝ids
        List<String> fanUids = Lists.newArrayList();
        Get relationGet = new Get(Bytes.toBytes(uid));
        relationGet.addFamily(Bytes.toBytes("fans"));
        Cell[] cells = relationTable.get(relationGet).rawCells();
        for (Cell cell : cells) {
            fanUids.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
        }
        //从粉丝们的收件箱中删除我这条微博的版本
        List<Delete> inboxDeletes = Lists.newArrayList();
        for (String fanUid : fanUids) {
            Delete inboxDelete = new Delete(Bytes.toBytes(fanUid));
            long timestamp = Long.valueOf(rowKey.split("_")[1]);
            inboxDelete.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), timestamp);
            inboxDeletes.add(inboxDelete);
        }
        inboxTable.delete(inboxDeletes);
        //删除我的微博
        client.deleteRow(InitTable.CONTENT_TABLE, rowKey);
        inboxTable.close();
        relationTable.close();
        //connection.close();
    }

    /**
     * 查看微博
     * 查看我关注的人的微博内容
     */
    public void showContent(String uid) throws IOException {
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions(5);
        Cell[] cells = inboxTable.get(inboxGet).rawCells();
        List<String> contentRowKeys = Lists.newArrayList();
        for (Cell cell : cells) {
            contentRowKeys.add(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        List<Get> contentGets = Lists.newArrayList();
        for (String rowKey : contentRowKeys) {
            Get contentGet = new Get(Bytes.toBytes(rowKey));
            contentGets.add(contentGet);
        }
        Result[] results = contentTable.get(contentGets);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(result.getRow()) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }


}
