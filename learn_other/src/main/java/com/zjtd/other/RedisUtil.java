package com.zjtd.other;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Set;

public class RedisUtil {

    private static JedisPool jedisPool=null;

    private static JedisSentinelPool jedisSentinelPool=null;

    public static void main(String[] args) {
        // 1 bind 注掉了？  2 protected-mode 是否关闭  或者 是否加了密码  3 防火墙

        //   Jedis jedis = new Jedis("hdp1",6379,60);



        Jedis jedis = getJedisFromSentinel();
        jedis.set("k20000", "v20000");

        System.out.println(jedis.get("k20000"));

        //    jedis.close();


    }

    public static  Jedis getJedis(){

        if(jedisPool==null){
            JedisPoolConfig jedisPoolConfig =new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间

            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数

            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            jedisPool=new JedisPool( jedisPoolConfig, "hdp1",6379 );
            return  jedisPool.getResource();

        }else{
            return   jedisPool.getResource();

        }

    }


    public static  Jedis getJedisFromSentinel() {

        if(jedisSentinelPool==null){
            JedisPoolConfig jedisPoolConfig =new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间

            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数

            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong


            Set<String> sentinelSet=new HashSet<>();
            sentinelSet.add("192.168.11.101:26379");

            jedisSentinelPool=new JedisSentinelPool("mymaster", sentinelSet,jedisPoolConfig);
            return  jedisSentinelPool.getResource();

        }else{
            return   jedisSentinelPool.getResource();

        }


    }

}
