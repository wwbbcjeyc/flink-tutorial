package com.zjtd.other;

import com.google.gson.internal.$Gson$Preconditions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisDemo {
    public static void main(String[] args) {
        /*Jedis jedis = new Jedis("127.0.0.1",6379);*/
        //private static volatile JedisPool jedisPool;

        //jedis.auth("shX$t9TdiLjM4nJu");
        //2> {"uv":2545,"event_sn":"flowCourseFormCodeInputClick","flow_id":"53394"}
        //2> {"uv":2578,"event_sn":"pageViewForFlow","flow_id":"51285"}
       /* System.out.println(jedis.get("pageViewForFlow_54976"));
        System.out.println(jedis.scard("pageViewForFlow_54976"));*/
         JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);  //最大连接数
        jedisPoolConfig.setMaxIdle(20);   //最大空闲
        jedisPoolConfig.setMinIdle(20);     //最小空闲
        jedisPoolConfig.setBlockWhenExhausted(true);  //连接耗尽是否等待
        jedisPoolConfig.setMaxWaitMillis(500);//忙碌时等待时长 毫秒
        jedisPoolConfig.setTestOnBorrow(true); //每次获得连接的进行测试

        /* JedisPool jedisPool = new JedisPool(jedisPoolConfig, "127.0.0.1", 6379);
         Jedis jedis = jedisPool.getResource();
        //kSystem.out.println(jedis.hlen("runoobkeys"));
        System.out.println(jedis.scard("runoobkeysets"));
        System.out.println(jedis.exists("runoobkeysets"));*/
        //flowCourseBulletShow
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "172.16.200.45", 6379, Protocol.DEFAULT_TIMEOUT,"shX$t9TdiLjM4nJu");

         Jedis jedis = jedisPool.getResource();
         Long result = jedis.scard("flowCourseBulletShow_49307");
        System.out.println(result);


    }
}
