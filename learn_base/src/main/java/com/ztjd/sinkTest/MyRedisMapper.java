package com.ztjd.sinkTest;

import com.ztjd.bean.SensorReading;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisMapper implements RedisMapper<SensorReading> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET,"sensor_tmp");

    }

    @Override
    public String getKeyFromData(SensorReading sensorReading) {
        return sensorReading.getId();
    }

    @Override
    public String getValueFromData(SensorReading sensorReading) {
        return sensorReading.getTemperature().toString();
    }
}
