package com.zitd.example.reflect;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.util.List;

@Slf4j
public class JDBCFunction extends RichSinkFunction <List<JSONObject>>{
    PreparedStatement ps;
    DruidDataSource dataSource;
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new DruidDataSource();
        connection = getConnection(dataSource);
       /* connection = getConnection();*/
        String sql = "insert into student (name,password,age) values(?, ?, ?);";
        if (this.connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

   /* private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
            con = DriverManager.getConnection("jdbc:mysql://172.16.0.115:3306/hrs?useSSL=false", "app", "Yc)E7aqYU6)AjW");
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }*/

    private Connection getConnection(DruidDataSource dataSource) {
       dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");

        dataSource.setUrl("jdbc:mysql://172.16.0.115:3306/hrs?useSSL=false");
        dataSource.setUsername("app");
        dataSource.setPassword("Yc)E7aqYU6)AjW");

        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
           con =  dataSource.getConnection();
            log.info("创建连接池：" + con);
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            log.info("-----------mysql get connection has exception , msg = " + e.getMessage());
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }


    @Override
    public void invoke(List<JSONObject> value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        for (JSONObject jsonObject : value) {
            System.out.println(jsonObject.getString("name"));
                ps.setString(1,jsonObject.getString("name"));
                ps.setString(2,jsonObject.getString("password"));
                ps.setInt(3,jsonObject.getInteger("age"));
                ps.addBatch();

        }
        int[] count = ps.executeBatch();
        log.info("成功了插入了" + count.length + "行数据");
        System.out.println("成功了插入了" + count.length + "行数据");
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }




}
