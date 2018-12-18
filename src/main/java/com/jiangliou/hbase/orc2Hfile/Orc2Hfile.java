package com.jiangliou.hbase.orc2Hfile;

import com.jiangliou.base.BaseMR;
import com.jiangliou.utils.Constants;
import com.jiangliou.utils.OrcFormat;
import com.jiangliou.utils.OrcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @ ClassName    :  Orc2Hfile
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/14
 */
public class Orc2Hfile extends BaseMR {

    /**
     * KEYIN, VALUEIN
     * 参考
     * public class OrcNewInputFormat extends InputFormat<NullWritable, OrcStruct>{
     * 		 public RecordReader<NullWritable, OrcStruct> createRecordReader(){
     * 				return new OrcRecordReader();
     * 		}
     *
     * }
     *
     * private static class OrcRecordReader extends RecordReader<NullWritable, OrcStruct> {
     *
     * }
     * KEYIN:NullWritable
     * VALUEIN:OrcStruct
     *
     * ------------------------------------
     *  KEYOUT, VALUEOUT
     *  生成hfile文件的设置,全在HFileOutputFormat2.configureIncrementalLoad() 里面
     *  不需要自己去写mapreduce生成hfile文件，直接调用上面方法的api
     *
     *  在configureIncrementalLoad()里面，根据不同的map输出value类型，设置不同的reduce处理
     *  本例，用的是map输出的是：Put.class，  reduce：PutSortReducer.class
     *
     *  public class PutSortReducer extends Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue>
     */

    public static class Orc2HfileMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>{
        OrcUtil orcUtil = new OrcUtil();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        ImmutableBytesWritable keyOut = new ImmutableBytesWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            orcUtil.setReadOrcInspector(OrcFormat.orcSchema);
        }

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            String aid = orcUtil.getOrcStructData(value, "aid");
            String pkgname = orcUtil.getOrcStructData(value, "pkgname");
            String uptimestr = orcUtil.getOrcStructData(value, "uptime");
            String type = orcUtil.getOrcStructData(value, "type");
            String country = orcUtil.getOrcStructData(value, "country");
            String gpcategory = orcUtil.getOrcStructData(value, "gpcategory");
            System.out.println("----------------------------------");
            System.out.println("aid         :" + aid        );
            System.out.println("pkgname     :" + pkgname    );
            System.out.println("uptime      :" + uptimestr     );
            System.out.println("type        :" + type       );
            System.out.println("country     :" + country    );
            System.out.println("gpcategory  :" + gpcategory );

            if(aid == null || uptimestr == null){
                context.getCounter("hainiu", "bad line num").increment(1L);
                return;
            }
            //获取更新时间
            long uptime = Long.parseLong(uptimestr) * 1000;
            //20141228112233
            uptimestr = sdf.format(new Date(uptime));
            //rowkey: aid_uptimestr
            String rowkey = aid + "_" + uptimestr;
            keyOut.set(Bytes.toBytes(rowkey));
            Put put = new Put(Bytes.toBytes(rowkey));
            if(null != pkgname){
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("pkgname"),Bytes.toBytes(pkgname));
            }
            if(type != null){
                put.addColumn(toBytes("cf"), toBytes("type"), toBytes(type));
            }
            if(country != null){
                put.addColumn(toBytes("cf"), toBytes("country"), toBytes(country));
            }
            if(gpcategory != null){
                put.addColumn(toBytes("cf"), toBytes("gpcategory"), toBytes(gpcategory));
            }
            context.write(keyOut,put);
        }
    }
    @Override
    public Job getJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, getJobNameWithTaskId());
        job.setJarByClass(Orc2Hfile.class);
        job.setMapperClass(Orc2HfileMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //读取orc文件的format
        job.setInputFormatClass(OrcNewInputFormat.class);
        //下面的代码都是围绕着HfileOutputFormat2.configureIncrmentalLoad()去写的
        Configuration hbaseConf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        TableName tableName = TableName.valueOf(conf.get(Constants.HBASE_TABLE_NAME_ATTR));
        //生成hfile文件的api
        HFileOutputFormat2.configureIncrementalLoad(job,conn.getTable(tableName),conn.getRegionLocator(tableName));
        FileInputFormat.addInputPath(job, getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));
        return job;
    }

    @Override
    public String getJobName() {
        return null;
    }
}
