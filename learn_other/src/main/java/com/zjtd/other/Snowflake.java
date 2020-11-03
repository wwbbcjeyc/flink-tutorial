package com.zjtd.other;


/**
 * @ClassName Snowflake
 * @Description 使用snowflake算法生成id
 */
public class Snowflake {

    //机群所占位数
    private final static int fleetBit = 5;
    //机器码所占位数
    private final static int machineBit = 5;
    //自增序列所占位数
    private final static int sequenceBit = 12;

    //机器码位移量
    private final static int machineDisplacement = sequenceBit;
    //机群位移量
    private final static int fleetDisplacement = machineBit + sequenceBit;
    //时间戳位移量
    private final static int timeDisplacement = fleetBit + machineBit + sequenceBit;

    //参照时间(2018-01-01 00:00:00)
    private final static long referenceTime = 1514736000000l;

    //自增序列最大值
    private final static int sequenceMax = -1^(-1<<12);

    //自增序列
    private static int sequence = 0;

    //上一次生成自增序列的时间
    private static long lastTimeStamp = -1l;

    /*
     * @Author boy
     * @Description 获取全局唯一id
     * @Date 2019/6/11 5:27 PM
     * @Param [fleetCode, machineCode]
     * @return long
     */
    public static synchronized long getNextId(int fleetCode,int machineCode){
        long currentTimeStamp = getCurrentTimeStamp();
        if (lastTimeStamp==currentTimeStamp){
            if (sequence>=sequenceMax){
                currentTimeStamp = getNextTimeStamp();
                lastTimeStamp = currentTimeStamp;
                sequence = 0;
            } else {
                sequence++;
            }

        } else if (lastTimeStamp<currentTimeStamp){
            sequence = 0;
            lastTimeStamp = currentTimeStamp;
        } else if (lastTimeStamp>currentTimeStamp){

        }
        long id = (currentTimeStamp-referenceTime)<<timeDisplacement |
                fleetCode<<fleetDisplacement |
                machineCode<<machineDisplacement |
                sequence;
        return id;

    }

    /*
     * @Author boy
     * @Description 获取当前时间戳，精确到毫秒
     * @Date 2019/6/11 5:26 PM
     * @Param []
     * @return long
     */
    private static long getCurrentTimeStamp(){
        return System.currentTimeMillis();
    }

    /*
     * @Author boy
     * @Description 当前毫秒自增序列已经达到最大值，等待获取下一毫秒
     * @Date 2019/6/11 5:27 PM
     * @Param []
     * @return long
     */
    private static long getNextTimeStamp(){
        long timeStamp = getCurrentTimeStamp();
        while (timeStamp<=lastTimeStamp){
            timeStamp = getCurrentTimeStamp();
        }
        return timeStamp;
    }



}
