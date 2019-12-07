package com.jj.hhase.exercise_weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * 创建三张表
 * content://微博表
 *      row:uid_timestamp//操作人
 *      family:info
 *          column:content
 *          value:user_content//微博发布内容
 *  relation://关系表
 *      row:uid
 *      family:attends,fans//已关注和粉丝
 *          column:uid//已关注和粉丝id，每个用户的关注和粉丝id均不同，这是非结构化列
 *          value:uid//这个value用不到
 *  inbox://收件箱表
 *      row:uid
 *      family:info//注意该表的cell展示最新的五个版本,注意存入cell的timestamp应该取自内容表的时间戳
 *          column:uid每个用户关注的人不同，非结构化列
 *          value:对应content表的row
 */
public class InitTable {
    public static String WEIBO_NAMESPACE = "ns_weibo";
    public static String CONTENT_TABLE = WEIBO_NAMESPACE+":content";
    public static String RELATION_TABLE = WEIBO_NAMESPACE+":relation";
    public static String INBOX_TABLE = WEIBO_NAMESPACE+":inbox";


    public static void main(String[] args) throws IOException {
//        createNameSpace();
//        createContent();
//        createRelation();
        createInbox();
    }

    private static void createInbox() throws IOException {
        Connection connection = WeiboFactory.getConnection();
        Admin admin = connection.getAdmin();
        HColumnDescriptor infoHcd2 = new HColumnDescriptor("info");
        infoHcd2.setBlockCacheEnabled(true);
        infoHcd2.setBlocksize(2*1024*1024);
        infoHcd2.setMinVersions(100);
        infoHcd2.setMaxVersions(100);
        HTableDescriptor inboxHtd = new HTableDescriptor(TableName.valueOf(INBOX_TABLE));
        inboxHtd.addFamily(infoHcd2);
        admin.createTable(inboxHtd);
        admin.close();
        connection.close();
    }

    private static void createRelation() throws IOException {
        Connection connection = WeiboFactory.getConnection();
        Admin admin = connection.getAdmin();
        HColumnDescriptor attendsHcd = new HColumnDescriptor("attends");
        attendsHcd.setBlockCacheEnabled(true);
        attendsHcd.setBlocksize(2*1024*1024);
        HColumnDescriptor fansHcd = new HColumnDescriptor("fans");
        fansHcd.setBlockCacheEnabled(true);
        fansHcd.setBlocksize(2*1024*1024);
        fansHcd.setMinVersions(1);
        fansHcd.setMaxVersions(1);
        HTableDescriptor htd2 = new HTableDescriptor(TableName.valueOf(RELATION_TABLE))
                .addFamily(attendsHcd)
                .addFamily(fansHcd);
        admin.createTable(htd2);
        admin.close();
        connection.close();
    }

    private static void createContent() throws IOException {
        Connection connection = WeiboFactory.getConnection();
        Admin admin = connection.getAdmin();
        HColumnDescriptor infoHcd = new HColumnDescriptor("info");
        infoHcd.setMinVersions(1);
        infoHcd.setMaxVersions(1);
        //设置块缓存
        infoHcd.setBlockCacheEnabled(true);
        infoHcd.setBlocksize(2*1024*1024);
        //设置压缩方式
//        infoHcd.setCompressionType(Compression.Algorithm.SNAPPY);
        HTableDescriptor infoHtd = new HTableDescriptor(TableName.valueOf(CONTENT_TABLE));
        infoHtd.addFamily(infoHcd);
        admin.createTable(infoHtd);
        admin.close();
        connection.close();
    }

    private static void createNameSpace() throws IOException {
        Connection connection = WeiboFactory.getConnection();
        Admin admin = connection.getAdmin();
        //构建命名空间
        NamespaceDescriptor nsd = NamespaceDescriptor
                .create(WEIBO_NAMESPACE)
                .addConfiguration("creator", "jj")
                .addConfiguration("createTime", System.currentTimeMillis() + "")
                .build();
        admin.close();
        connection.close();
    }

}
