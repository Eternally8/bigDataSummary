package com.jiangliou.hbase.orc;

import com.jiangliou.utils.JobRunResult;
import com.jiangliou.utils.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ ClassName    :  Text2AvroJob
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/18
 */
public class Text2AvroJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //获取Configuration对象
        Configuration conf = getConf();


        //创建任务链对象
        JobControl jobc = new JobControl("text2avro");

        Text2Avro avro = new Text2Avro();

        //只需要赋值一次就行
        avro.setConf(conf);

        ControlledJob orcCJob = avro.getControlledJob();

        jobc.addJob(orcCJob);

        JobRunResult result = JobRunUtil.run(jobc);
        result.print(true);


        return 0;
    }
    public static void main(String[] args) throws Exception {
//		-Dtask.id=1218 -Dtask.input.dir=/tmp/avro/input -Dtask.base.dir=/tmp/avro
        System.exit(ToolRunner.run(new Text2AvroJob(), args));
    }
}
