/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ztjd.sinkTest;

import org.apache.flink.api.java.io.jdbc.AbstractJDBCOutputFormat;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Row
 * @see DriverManager
 */
public class ClickHouseSink01 extends AbstractJDBCOutputFormat<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSink01.class);

    private final String query;
    private final int batchInterval;
    private final int[] typesArray;
    private long lastFlushTime = new Date().getTime();
    private long maxFlushTime;

    private PreparedStatement upload;
    private int batchCount = 0;

    public ClickHouseSink01(String username, String password, String drivername,
                          String dbURL, String query, int batchInterval, int[] typesArray) {
        super(username, password, drivername, dbURL);
        this.query = query;
        this.batchInterval = batchInterval;
        this.typesArray = typesArray;
    }

    public ClickHouseSink01(String username, String password, String drivername,
                          String dbURL, String query, int batchInterval, int[] typesArray, long maxFlushTime) {
        super(username, password, drivername, dbURL);
        this.query = query;
        this.batchInterval = batchInterval;
        this.typesArray = typesArray;
        this.maxFlushTime = maxFlushTime;
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     *
     * @param taskNumber The number of the parallel instance.
     * @throws IOException Thrown, if the output could not be opened due to an
     * I/O problem.
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            establishConnection();
            upload = connection.prepareStatement(query);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        try {
            setRecordToStatement(upload, typesArray, row);
            upload.addBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Preparation of JDBC statement failed.", e);
        }
        batchCount++;
        if (batchCount >= batchInterval) {
            // execute batch
            flush();
        } else if ((new Date().getTime() - lastFlushTime) > maxFlushTime) {
            flush();
        }
    }

    /**
     * 刷新数据入库
     */
    void flush() {
        try {
            lastFlushTime = new Date().getTime(); //获取当前提交时间为最新时间
            upload.executeBatch();
            batchCount = 0;
        } catch (SQLException e) {
            throw new RuntimeException("Execution of JDBC statement failed.", e);
        }
    }

    int[] getTypesArray() {
        return typesArray;
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        if (upload != null) {
            flush();
            try {
                upload.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                upload = null;
            }
        }

        closeDbConnection();
    }

    /**
     * 默认初始化
     * @return JDBCOutputFormatBuilder
     */
    public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
        return new JDBCOutputFormatBuilder();
    }

    /**
     * Builder for a {@link ClickHouseSink}.
     */
    public static class JDBCOutputFormatBuilder {
        private String username;
        private String password;
        private String drivername;
        private String dbURL;
        private String query;
        private int batchInterval;
        private int[] typesArray;
        private long maxFlushTime;

        protected JDBCOutputFormatBuilder() { }

        /**
         *
         * @param username 用户名
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         *
         * @param password 密码
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         *
         * @param drivername  drivername
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setDrivername(String drivername) {
            this.drivername = drivername;
            return this;
        }

        /**
         *
         * @param dbURL dbURL
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setDBUrl(String dbURL) {
            this.dbURL = dbURL;
            return this;
        }

        /**
         *
         * @param query query
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setQuery(String query) {
            this.query = query;
            return this;
        }

        /**
         *
         * @param batchInterval 提交批次
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setBatchInterval(int batchInterval) {
            this.batchInterval = batchInterval;
            return this;
        }

        /**
         *
         * @param typesArray 类型数组
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setSqlTypes(int[] typesArray) {
            this.typesArray = typesArray;
            return this;
        }

        /**
         *
         * @param maxFlushTime 最大刷新时间
         * @return JDBCOutputFormatBuilder
         */
        public JDBCOutputFormatBuilder setMaxFlushTime(long maxFlushTime) {
            this.maxFlushTime = maxFlushTime;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JDBCOutputFormat
         */
        public ClickHouseSink01 finish() {
            if (this.username == null) {
                LOG.info("Username was not supplied.");
            }
            if (this.password == null) {
                LOG.info("Password was not supplied.");
            }
            if (this.dbURL == null) {
                throw new IllegalArgumentException("No database URL supplied.");
            }
            if (this.query == null) {
                throw new IllegalArgumentException("No query supplied.");
            }
            if (this.drivername == null) {
                throw new IllegalArgumentException("No driver supplied.");
            }

            return new ClickHouseSink01(
                    username, password, drivername, dbURL,
                    query, batchInterval, typesArray, maxFlushTime);
        }
    }

}
