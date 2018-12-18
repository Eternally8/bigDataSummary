package com.jiangliou.hbase.orc2Hfile;

import com.jiangliou.utils.Constants;
import com.jiangliou.utils.JobRunResult;
import com.jiangliou.utils.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ ClassName    :  Orc2HfileJob
 * @ Description  :  TODO
 * @ author       :  jlo
 * @ Date:  2018/12/16
 */
public class Orc2HfileJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //获取Configuration对象
        Configuration conf = getConf();
        String tableName = conf.get(Constants.HBASE_TABLE_NAME_ATTR);
        //创建hbase表
        // 通过传递-D参数判断需不需要创建 -Dcreate.table=true
        String str = conf.get("create.table");
        str = (str == null || str.equals("null") || str.equals("") ? "false" : str);
        boolean createTable = Boolean.getBoolean(str);
        if(createTable){
            //参考hbase org.apache.hadoop.hbase.util.RegionSplitter
            //user_install_status_aid_split com.hainiuxy.hbase.TableSplit -c 2 -f cf
            String splitClassName = "com.jiangliou.hbase.orc2Hfile.TableSplit";
            String[] s = new String[]{tableName, splitClassName, "-c", "2", "-f", "cf"};
            RegionSplitter.main(s);
        }
        //创建任务链对象
        JobControl jobc = new JobControl("orc2hife");
        Orc2Hfile orc = new Orc2Hfile();
        //只需要赋值一次就行
        orc.setConf(conf);
        ControlledJob orcCJob = orc.getControlledJob();
        jobc.addJob(orcCJob);
        JobRunResult result = JobRunUtil.run(jobc);
        result.print(true);
        //将导入hbase表的命令集成到任务链上，需要把导入逻辑放到已经生成hfile文件之后
        ///user/hadoop/hbase/orc2hfile_1211 user_install_status
        //参数1：带有列族目录的hfile文件的输出目录
        //String outputPath = orc.getJobOutputPath(orc.getJobNameWithTaskId()).toString();
        //参数2：hbase表名称
        //LoadIncrementalHFiles.main(new String[]{outputPath, tableName});


        return 0;
    }

    public static void main(String[] args) throws Exception {
//		-Dtask.id=1211 -Dtask.input.dir=/tmp/orc/input -Dtask.base.dir=/tmp/orc -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
        System.exit(ToolRunner.run(new Orc2HfileJob(), args));
    }
}
