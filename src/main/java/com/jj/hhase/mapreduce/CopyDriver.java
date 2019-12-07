package com.jj.hhase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CopyDriver implements Tool {
    private Configuration conf;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CopyDriver(),args);
    }

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(CopyDriver.class);
        //不使用hadoop原生API初始化map
        TableMapReduceUtil.initTableMapperJob(
                "fruit",//表名
                new Scan(),//扫描器 靠这两项控制读取表的数据 相当于hadoop的FileInputFormat
                //**可以指定多个scan通过设定属性添加表名
//        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("tableName"));
                ReadMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",//输出的表名 相当于hadoop的FileOutputFormat
                CopyReducer.class,
                job);
        job.waitForCompletion(true);
        return 0;
    }

    public void setConf(Configuration configuration) {
        //使用Hbase配置创建
        this.conf = HBaseConfiguration.create(configuration);
    }

    public Configuration getConf() {
        return conf;
    }
}
