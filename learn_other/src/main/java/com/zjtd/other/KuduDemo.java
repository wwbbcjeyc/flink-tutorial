package com.zjtd.other;

import org.apache.kudu.client.*;

public class KuduDemo {

    public static void main(String[] args) throws KuduException {
        // master地址
        final String masteraddr = "cdh001-cvm-bj3.qcloud.xiaobang.xyz";
        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
        //打开kudu表
        KuduTable student = client.openTable("impala::ods_xb.o_metis_term_info_i");
        //创建scanner扫描
        KuduScanner scanner = client.newScannerBuilder(student).build();


        //遍历数据
        while (scanner.hasMoreRows()){
            RowResultIterator rowResults = scanner.nextRows();
            int numRows = rowResults.getNumRows();
            System.out.println(numRows);

            for (RowResult rowResult : scanner.nextRows()) {
                System.out.println(rowResult.rowToString()) ;
            }
        }

    }
}
