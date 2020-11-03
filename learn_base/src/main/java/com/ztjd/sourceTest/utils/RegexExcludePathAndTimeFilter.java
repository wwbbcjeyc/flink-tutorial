package com.ztjd.sourceTest.utils;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

public class RegexExcludePathAndTimeFilter extends FilePathFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegexExcludePathAndTimeFilter.class);
    //时间开始过滤
    private final String start;
    //时间结束过滤
    private final String end;
    public RegexExcludePathAndTimeFilter(String start,String end) {
        this.start=start;
        this.end=end;
    }

    @Override
    public boolean filterPath(Path filePath) {
        String data[]=filePath.getName().split("/");
        String date=data[data.length-1];
        return TimeTools.checkDate(start, end, date);
    }
}
/**日期比较的工具类**/
class TimeTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeTools.class);
    final static String DATE_FORMAT = "yyyy-MM-dd";
    final static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
    public static boolean cnull(String checkString) {
        if (checkString == null || checkString.equals("")) {
            return false;
        }
        return true;
    }
    /**
     * @param start 开始时间
     * @param end   结束时间
     * @param path  比较的日期路径
     **/
    public static boolean checkDate(String start, String end, String path) {
        long startlong = 0;
        long endlong = 0;
        long pathlong = 0;
        try {
            if (cnull(start)) {
                startlong = sdf.parse(start).getTime();
            }
            if (cnull(end)) {
                endlong = sdf.parse(end).getTime();
            }
            if (cnull(path)) {
                pathlong = sdf.parse(path.substring(3)).getTime();
            }
            //当end日期为空时，只取start+的日期
            if (end == null || end.equals("")) {
                if (pathlong >= startlong) {
                    return false;
                } else {
                    return true;
                }
            } else {//当end不为空时，取日期范围直接比较
                //过滤在规定的日期范围之内
                if (pathlong >= startlong && pathlong <= endlong) {
                    return false;
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            LOGGER.error("转化异常"+ e.getStackTrace().toString());
            return false;
        }

    }
}
