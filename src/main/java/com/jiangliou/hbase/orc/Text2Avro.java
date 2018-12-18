package com.jiangliou.hbase.orc;

import com.jiangliou.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ ClassName    :  Text2Avro
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/18
 */
public class Text2Avro extends BaseMR {

    public static Schema schema = null;

    public static Schema.Parser parse = new Schema.Parser();

    /**
     * 通过public class AvroKeyOutputFormat<T> extends AvroOutputFormatBase<AvroKey<T>, NullWritable>
     *
     * 泛型GenericRecord：代表avro一行的对象
     * keyOut：AvroKey<GenericRecord>
     *
     * valueout:NullWritable
     *
     */
    public static class Text2AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {


        @Override
        protected void setup(Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>.Context context)
                throws IOException, InterruptedException {
            //根据user_install_status.avro文件内的格式，生成指定格式的schema对象
            schema = parse.parse(Text2Avro.class.getResourceAsStream("/user_install_status.avro"));

        }
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            //8d30437e6fbf2f70	com.sohu.inputmethod.sogou	1414379673	2	CN	tools
            String[] splits = line.split("\t");
            if(splits == null || splits.length != 6){
                context.getCounter("hainiu", "bad line num").increment(1L);
                return;
            }

            String aid = splits[0];
            String pkgname = splits[1];
            String country = splits[4];
            String gpcategory = splits[5];


            //根据创建的Schema对象，创建一行的对象
            GenericRecord record = new GenericData.Record(Text2Avro.schema);
            record.put("aid", aid);
            record.put("pkgname", pkgname);
            record.put("country", country);
            record.put("gpcategory", gpcategory);
            //将avro格式 写出
            context.write(new AvroKey<GenericRecord>(record), NullWritable.get());


        }
    }

    @Override
    public Job getJob(Configuration conf) throws IOException {
        //		// 开启reduce输出压缩
//		conf.set(FileOutputFormat.COMPRESS, "true");
//		// 设置reduce输出压缩格式
//		conf.set(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class.getName());

        Job job = Job.getInstance(conf, getJobNameWithTaskId());

        job.setJarByClass(Text2Avro.class);

        job.setMapperClass(Text2AvroMapper.class);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

//		无reduce
        job.setNumReduceTasks(0);

        //设置输出的format
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        //根据user_install_status.avro文件内的格式，生成指定格式的schema对象
        schema = parse.parse(Text2Avro.class.getResourceAsStream("/user_install_status.avro"));

        //设置avro文件的输出
        AvroJob.setOutputKeySchema(job, schema);

        FileInputFormat.addInputPath(job, getFirstJobInputPath());

        FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskId()));
        return job;

    }

    @Override
    public String getJobName() {
        return "text2avro";
    }
}
