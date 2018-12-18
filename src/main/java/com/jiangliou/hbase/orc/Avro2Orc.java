package com.jiangliou.hbase.orc;

import com.jiangliou.base.BaseMR;
import com.jiangliou.utils.OrcFormat;
import com.jiangliou.utils.OrcUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ ClassName    :  Avro2Orc
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/18
 */
public class Avro2Orc extends BaseMR {

    public static Schema schema = null;

    public static Schema.Parser parse = new Schema.Parser();

    public static  class Avro2OrcMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,NullWritable, Writable>{
        OrcUtil orcUtil = new OrcUtil();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            orcUtil.setWriteOrcInspector(OrcFormat.orcSchema);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            //得到一行的对象
            GenericRecord datum = key.datum();
            String aid = (String) datum.get("aid");
            String pkgname = (String) datum.get("pkgname");
            String country = (String) datum.get("country");
            String gpcategory = (String) datum.get("gpcategory");
            orcUtil.addAttr(aid,pkgname,country,gpcategory);
            Writable w = orcUtil.serialize();
            context.write(NullWritable.get(),w);

        }
    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        //关闭map的推测执行，使得一个map处理 一个region的数据
        conf.set("mapreduce.map.spedulative", "false");
        //设置orc文件snappy压缩
        conf.set("orc.compress", CompressionKind.SNAPPY.name());
        //设置orc文件 有索引
        conf.set("orc.create.index", "true");
        Job job = Job.getInstance(conf, getJobNameWithTaskId());
        job.setJarByClass(Avro2Orc.class);
        job.setMapperClass(Avro2OrcMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Writable.class);
        //无reduce
        job.setNumReduceTasks(0);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        //根据user_install_status.avro文件内的格式，生成指定格式的schema对象
        schema = parse.parse(Avro2Orc.class.getResourceAsStream("/user_install_status.avro"));
        AvroJob.setInputKeySchema(job,schema);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        FileInputFormat.addInputPath(job, getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskId()));
        return job;
    }

    @Override
    public String getJobName() {
        return "Avro2Orc";
    }
}
