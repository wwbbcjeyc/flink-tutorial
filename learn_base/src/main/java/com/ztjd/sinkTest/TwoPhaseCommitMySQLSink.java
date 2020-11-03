package com.ztjd.sinkTest;

/**
 * 使用关系型数据库 MySQL，开启 CheckPoint 机制的前提下
 * ，为了保证前一次 CheckPoint 成功后到这次 CheckPoint 成功之前这段时间内的数据不丢失，如果执行到一半过程任务失败了
 * ，从而导致前一次CheckPoint成功后到任务失败前的数据已经存储到了MySQL,然而这部分数据并没有写入到 CheckPoint。
 * 如果任务重启后，前一次CheckPoint成功后到任务失败前的数据便会再次写入MySQL,从而导致数据重复的问题。
 * 这种情况，便使用到了 TwoPhaseCommitSinkFunction类，以此来实现 MySQL 关系型数据库的二阶提交。
 */


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;


/**
 * 自定义 Kafka to mysql,继承TwoPhaseCommitSinkFunction,实现两阶段提交
 */
public class TwoPhaseCommitMySQLSink extends TwoPhaseCommitSinkFunction<Tuple2<String,Integer>,Connection,Void>{

    private static final Logger log = LoggerFactory.getLogger(TwoPhaseCommitMySQLSink.class);

    public TwoPhaseCommitMySQLSink(){
        super(new KryoSerializer<>(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     * @param connection
     * @param tp
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, Tuple2<String,Integer> tp, Context context) throws Exception {
        log.info("start invoke...");
        String value = tp.f0;
        Integer total = tp.f1;
        String sql = "insert into `t_test` (`value`,`total`,`insert_time`) values (?,?,?)";
        log.info("====执行SQL:{}===",sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, value);
        ps.setInt(2, total);
        ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
        log.info("要插入的数据:{}----{}",value,total);
        if (ps != null) {
            String sqlStr = ps.toString().substring(ps.toString().indexOf(":")+2);
            log.error("执行的SQL语句:{}",sqlStr);
        }
        //执行insert语句
        ps.execute();
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        log.info("start beginTransaction.......");
        String url = "jdbc:mysql://192.168.86.245:3306/rh_datatool?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "123456");
        return connection;
    }

    /**
     *预提交，这里预提交的逻辑在invoke方法中
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        log.info("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        log.info("start commit...");
        DBConnectUtil.commit(connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        log.info("start abort rollback...");
        DBConnectUtil.rollback(connection);
    }



}
