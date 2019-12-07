package com.jj.hhase.mapreduce;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

/**
 * ImmutableBytesWritable 是key也就是rowKey
 * Result是value就可以读取一行数据
 * 通过context再把key和put输出到reducer
 */
public class ReadMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //通过put构建新行
        Put put = new Put(key.get());
        //读取表该行所有单元格添加到新行中
        Cell[] cells = value.rawCells();
        for(Cell cell : cells){
            put.add(cell);
        }
        //最后输出
        context.write(key,put);
    }
}
