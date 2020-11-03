package com.ztjd.flhive;/*
package com.ztjd.flhive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class flinkWhive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "/Users/admin/app/apache-hive-1.2.2-bin/conf";
        String version         = "1.2.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
        tableEnv.sqlUpdate("insert into student values('0702013','test');");
        tableEnv.execute("test");
        bsEnv.execute("test");




    }
}
*/
