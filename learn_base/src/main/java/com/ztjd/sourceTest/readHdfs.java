package com.ztjd.sourceTest;

import com.ztjd.sourceTest.utils.RegexExcludePathAndTimeFilter;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;

//只读取test2目录下的2019-12-26到2019-12-27号所有的数据
public class readHdfs {
    public static void main(String[] args) throws Exception {
        //初始化任务参数
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        FileInputFormat fileInputFormat = new TextInputFormat(new Path("hdfs://172.16.200.47:9000/warehouse/"));
        fileInputFormat.setNestedFileEnumeration(true);
        //过滤掉条件为true
       // fileInputFormat.setFilesFilter(new RegexExcludePathAndTimeFilter("2019-12-26","2019-12-27"));
        DataSet<String> dataSet =env.createInput(fileInputFormat);


        //dataSet.output(new HdfsTrainSinktest());
        env.execute("offline_train");
    }


}
