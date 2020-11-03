package com.zjtd.other;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @ClassName SnowflakeDoubleTest
 * @Description 使用LRU算法验证snowflake算法生成的id是有有重复
 *      生成1亿个id，LRU的深度为1万。每次查询最近的1万个id和新生成的是否会用重复
 * @Author boy
 * @Date 2019/6/13 12:32 PM
 */
public class SnowflakeDoubleTest {

    //队列的深度
    private static int deep = 10000;
    //需要生成的id数量
    private static long num = 100000000l;
    //重复id的数量
    private static int repeatNum = 0;
    public static void main(String args[]){
        LRU lru = new LRU(deep);
        for(int i=0;i<num;i++){
            long id = Snowflake.getNextId(1,2);
            if(lru.get(id)!=null){
                System.out.println(id);
                repeatNum++;
            }else {
                lru.put(id,id);
            }
        }
        System.out.println("重复的id数为：" + repeatNum);
    }
}

/**
 * @ClassName LRU
 * @Description 基于LinkedHashMap的LRU算法
 */
class LRU extends LinkedHashMap {

    //缓存长度
    int length;

    public LRU(int length){
        super(length,0.75f,true);
        this.length = length;
    }

    /*
     * @Author boy
     * @Description LRU算法的关键方法，超过最大长度就移除尾部元素
     * @Date 2019/6/13 12:24 PM
     * @Param [map]
     * @return boolean
     */
    @Override
    public boolean removeEldestEntry(Map.Entry map){
        return size()>length;
    }
}

