package com.jiangliou.hbase.scanHbase2Orc;

import com.jiangliou.utils.JobRunResult;
import com.jiangliou.utils.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.InputStream;
import java.io.OutputStream;

/**
 * @ ClassName    :  ScanHbase2OrcJob
 * @ Description  :  scanhbase表生成orc文件任务链
 * @ author       :  jlo
 * @ Date:  2018/12/18
 */
public class ScanHbase2OrcJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //创建任务链对象
        JobControl jobc = new JobControl("scanhbase2orc");
        ScanHbase2Orc orc = new ScanHbase2Orc();
        //只需要赋值一次就行
        orc.setConf(conf);
        ControlledJob orcCJob = orc.getControlledJob();
        jobc.addJob(orcCJob);
        JobRunResult result = JobRunUtil.run(jobc);
        result.print(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ScanHbase2OrcJob(), args));
    }
}
