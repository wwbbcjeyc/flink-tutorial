package com.ztjd.connectors.clickhouse;

import com.ztjd.connectors.clickhouse.applied.ClickhouseSinkBuffer;
import com.ztjd.connectors.clickhouse.applied.ClickhouseSinkManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
/**
 * @Author wangwenbo
 * @Date 2020/11/24 12:12 上午
 * @Version 1.0
 */
public class ClickhouseSink extends RichSinkFunction<String> {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSink.class);

    private static final Object DUMMY_LOCK = new Object();

    private final Properties localProperties;

    private volatile static transient ClickhouseSinkManager sinkManager;
    private transient ClickhouseSinkBuffer clickhouseSinkBuffer;

    public ClickhouseSink(Properties properties) {
        this.localProperties = properties;
    }

    @Override
    public void open(Configuration config) {
        if (sinkManager == null) {
            synchronized (DUMMY_LOCK) {
                if (sinkManager == null) {
                    Map<String, String> params = getRuntimeContext()
                            .getExecutionConfig()
                            .getGlobalJobParameters()
                            .toMap();

                    sinkManager = new ClickhouseSinkManager(params);
                }
            }
        }

        clickhouseSinkBuffer = sinkManager.buildBuffer(localProperties);
    }

    /**
     * Add csv to buffer
     *
     * @param recordAsCSV csv-event
     */
    @Override
    public void invoke(String recordAsCSV, Context context) {
        try {
            clickhouseSinkBuffer.put(recordAsCSV);
        } catch (Exception e) {
            logger.error("Error while sending data to Clickhouse, record = {}", recordAsCSV, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (clickhouseSinkBuffer != null) {
            clickhouseSinkBuffer.close();
        }

        if (sinkManager != null) {
            if (!sinkManager.isClosed()) {
                synchronized (DUMMY_LOCK) {
                    if (!sinkManager.isClosed()) {
                        sinkManager.close();
                    }
                }
            }
        }

        super.close();
    }
}

