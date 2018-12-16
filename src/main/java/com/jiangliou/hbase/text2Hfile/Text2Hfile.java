package com.jiangliou.hbase.text2Hfile;

import com.jiangliou.base.BaseMR;
import com.jiangliou.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @ ClassName    :  Text2Hfile
 * @ Description  :  读取text文件,导出hfile文件
 * @ author       :  jlo
 * @ Date:  2018/12/14
 */
public class Text2Hfile extends BaseMR {
    /**
     * @ Author jlo
     * @ Description 内部类map方法
     * @ Date 20:08 2018/12/14
     * @ Param
     * @ return
     **/
    public static class Text2HfileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
        ImmutableBytesWritable keyOut = new ImmutableBytesWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split(";");
            //判断数据是否有错
            if(null == splits || splits.length != 4){
                context.getCounter("badHome", "bad line num").increment(1L);
                return;
            }
            String id = splits[0];
            String name = splits[1];
            String sex = splits[2];
            String age = splits[3];
            System.out.println("----------------------------------");
            System.out.println("id         :" + id        );
            System.out.println("name     :" + name    );
            System.out.println("sex      :" + sex     );
            System.out.println("age        :" + age       );
            //如果rowKey是1位 ,将1改为01
            String rowKey = id.length() == 1 ? "0"+ id :id;
            keyOut.set(Bytes.toBytes(rowKey));
            Put put = new Put(Bytes.toBytes(rowKey));
            if(null != name){
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(name));
            }
            if(sex != null){
                put.addColumn(toBytes("cf"), toBytes("sex"), toBytes(sex));
            }
            if(age != null){
                put.addColumn(toBytes("cf"), toBytes("age"), toBytes(age));
            }
            context.write(keyOut,put);
        }
    }




    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, getJobNameWithTaskId());
        job.setJarByClass(Text2Hfile.class);
        job.setMapperClass(Text2HfileMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //下面的代码都是围绕着HFileOutputFormat2.configureIncrementalLoad()去写的
        Configuration hbaseConf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        TableName tableName = TableName.valueOf(conf.get(Constants.HBASE_TABLE_NAME_ATTR));
        //生成hfile文件的api
        HFileOutputFormat2.configureIncrementalLoad(job, conn.getTable(tableName), conn.getRegionLocator(tableName));
        FileInputFormat.addInputPath(job, getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));
        return job;
    }

    @Override
    public String getJobName() {
        return "Text2Hfile";
    }
}
