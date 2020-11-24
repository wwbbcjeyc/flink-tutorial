package com.ztjd.connectors.clickhouse.applied;

import com.ztjd.connectors.clickhouse.model.ClickhouseSinkCommonParams;
import com.ztjd.connectors.clickhouse.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @Author wangwenbo
 * @Date 2020/11/24 12:08 上午
 * @Version 1.0
 */
public class ClickhouseSinkScheduledChecker implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSinkScheduledChecker.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final List<ClickhouseSinkBuffer> clickhouseSinkBuffers;
    private final ClickhouseSinkCommonParams params;

    public ClickhouseSinkScheduledChecker(ClickhouseSinkCommonParams props) {
        clickhouseSinkBuffers = new ArrayList<>();
        params = props;

        ThreadFactory factory = ThreadUtil.threadFactory("clickhouse-writer-checker");
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(factory);
        scheduledExecutorService.scheduleWithFixedDelay(getTask(), params.getTimeout(), params.getTimeout(), TimeUnit.SECONDS);
        logger.info("Build Sink scheduled checker, timeout (sec) = {}", params.getTimeout());
    }

    public void addSinkBuffer(ClickhouseSinkBuffer clickhouseSinkBuffer) {
        synchronized (this) {
            clickhouseSinkBuffers.add(clickhouseSinkBuffer);
        }
        logger.debug("Add sinkBuffer, target table = {}", clickhouseSinkBuffer.getTargetTable());
    }

    private Runnable getTask() {
        return () -> {
            synchronized (this) {
                logger.debug("Start checking buffers. Current count of buffers = {}", clickhouseSinkBuffers.size());
                clickhouseSinkBuffers.forEach(ClickhouseSinkBuffer::tryAddToQueue);
            }
        };
    }

    @Override
    public void close() throws Exception {
        ThreadUtil.shutdownExecutorService(scheduledExecutorService);
    }
}
