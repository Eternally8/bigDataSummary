package com.jiangliou.hbase.text2Avro;

import com.jiangliou.base.BaseMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ ClassName    :  Text2Avro
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/18
 */
public class Text2Avro extends BaseMR {

    public static class Text2AvroMapper extends Mapper{

    }


    @Override
    public Job getJob(Configuration conf) throws IOException {
        return null;
    }

    @Override
    public String getJobName() {
        return null;
    }
}
