package com.jiangliou.hbase.text2Hfile;

import com.jiangliou.utils.JobRunResult;
import com.jiangliou.utils.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ ClassName    :  Text2HfileJob
 * @ Description  :  工作链(采用模板)
 * @ author       :  jlo
 * @ Date:  2018/12/14
 */
public class Text2HfileJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //获取Configuration
        Configuration conf = getConf();
        //创建任务链对象
        JobControl jobc = new JobControl("text2hfile");
        Text2Hfile text = new Text2Hfile();
        //只需要赋值一次就行
        text.setConf(conf);
        ControlledJob textCjob = text.getControlledJob();
        jobc.addJob(textCjob);
        JobRunResult result = JobRunUtil.run(jobc);
        result.print(true);
        //将导入hbase表的命令集成到任务链上，需要把导入逻辑放到已经生成hfile文件之后
//		///user/hadoop/hbase/orc2hfile_1211 user_install_status
//		//参数1：带有列族目录的hfile文件的输出目录
//		String outputPath = orc2Hfile.getJobOutputPath(orc2Hfile.getJobNameWithTaskId()).toString();
//		//参数2：hbase表名称
//
//		LoadIncrementalHFiles.main(new String[]{outputPath, tableName});

        return 0;
    }

    public static void main(String[] args) throws Exception {
        //-Dtask.id=1208 -Dhbase.table.name=hbase_student -Dtask.input.dir=/tmp/hbase/input -Dtask.base.dir=/tmp/hbase -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
        System.out.println(ToolRunner.run(new Text2HfileJob(),args));
    }
}
