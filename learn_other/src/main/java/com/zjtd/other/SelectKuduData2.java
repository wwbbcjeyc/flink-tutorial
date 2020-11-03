package com.zjtd.other;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

public class SelectKuduData2
{
    private static String tableName = "impala::dwa_xb.a_financial_live_chatroom_test";

    private static KuduClient client = new KuduClient.KuduClientBuilder("cdh001-cvm-bj3.qcloud.xiaobang.xyz").defaultAdminOperationTimeoutMs(60000).build();


    // 获取需要查询数据的列
    private static List<String> projectColumns = new ArrayList<String>();

    private static KuduTable table;
    private static KuduScanner.KuduScannerBuilder builder;
    private static KuduScanner scanner;
    private static KuduPredicate predicate1;
    private static KuduPredicate predicate2;

    public static void main(String[] args) throws KuduException
    {
        try
        {
            //select 查询字段名
            projectColumns.add("term_id"); //字段名
            projectColumns.add("class_sn");
            projectColumns.add("user_id");
            table = client.openTable(tableName);

            // 简单的读取 newScannerBuilder(查询表)  setProjectedColumnNames(指定输出列)  build()开始扫描
           //KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();

            builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);
            /**
             * 设置搜索的条件 where 条件过滤字段名
             * 如果不设置，则全表扫描
             */
            //下面的条件过滤 where user_id = xxx and day = xxx;
            int term_id = 2004718;
            String class_sn = "7179618";
            //比较方法ComparisonOp：GREATER、GREATER_EQUAL、EQUAL、LESS、LESS_EQUAL
            predicate1 = predicate1.newComparisonPredicate(table.getSchema().getColumn("term_id"),
                    KuduPredicate.ComparisonOp.EQUAL, term_id);
            predicate2 = predicate2.newComparisonPredicate(table.getSchema().getColumn("class_sn"),
                    KuduPredicate.ComparisonOp.EQUAL, class_sn);

            builder.addPredicate(predicate1);
            builder.addPredicate(predicate2);

            // 开始扫描
            scanner = builder.build();

            long sum = 0L;
            while (scanner.hasMoreRows())
            {
                RowResultIterator results = scanner.nextRows();
                /*
                  RowResultIterator.getNumRows()
                        获取此迭代器中的行数。如果您只想计算行数，那么调用这个函数并跳过其余的。
                        返回：此迭代器中的行数
                        如果查询不出数据则 RowResultIterator.getNumRows() 返回的是查询数据的行数，如果查询不出数据返回0
                 */
                // 每次从tablet中获取的数据的行数
                int numRows = results.getNumRows();
                sum +=numRows;
                System.out.println("numRows count is : " + numRows);
                while (results.hasNext()) {
                    RowResult result = results.next();
                    //System.out.println(result.rowToString());
                    long age1 = result.getLong(0);
                    String sex1 = result.getString(1);
                    long password = result.getLong(2);

                    System.out.println("age is : " + age1 + "  ===  sex: " + sex1 + " password:" +password );
                }
                /*System.out.println("--------------------------------------");
                System.out.println("内"+sum);*/
            }
            System.out.println("total:"+sum);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            scanner.close();
            client.close();
        }
    }
}
