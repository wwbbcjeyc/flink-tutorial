package com.zjtd.other;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class DateTimeDemo {
    public static void main(String[] args) {

        /*LocalDateTime midnight = LocalDateTime.ofInstant(new Date().toInstant(),
                ZoneId.systemDefault()).plusDays(15).withHour(0).withMinute(0)
                .withSecond(0).withNano(0);

        LocalDateTime currentDateTime = LocalDateTime.ofInstant(new Date().toInstant(),
                ZoneId.systemDefault());

        LocalDate today = LocalDate.now();

        System.out.println(new Date());
        System.out.println(midnight);
        System.out.println(currentDateTime);
        System.out.println(ChronoUnit.SECONDS.between(currentDateTime, midnight));
        System.out.println((int)ChronoUnit.SECONDS.between(currentDateTime, midnight));
        System.out.println(today);*/

        /*LocalDate now = LocalDate.now();
        System.out.println(now);
        LocalTime now1 = LocalTime.now();
        System.out.println(now1);*/

       /* LocalDateTime localDateTime = LocalDateTime.now();
        // 将当前时间转为时间戳
        long second = localDateTime.toEpochSecond(ZoneOffset.ofHours(8));
        // 1596084723
        System.out.println(second);
        System.out.println(localDateTime);*/

      /*  //获得时间戳
        long second = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).getEpochSecond();
        // 将时间戳转为当前时间
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(second, 0, ZoneOffset.ofHours(8));
        // 2020-02-03T13:30:44
        System.out.println(localDateTime);*/

        //毫秒
        //获得时间戳
       /* long milliseconds = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println(milliseconds);
        // 将时间戳转为当前时间
         LocalTime localTime = Instant.ofEpochMilli(milliseconds).atZone(ZoneOffset.ofHours(8)).toLocalTime();
        // 2020-02-03
        System.out.println(localTime);*/

        //秒
        //获得时间戳
        /*long seconds = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).getEpochSecond();
        // 将时间戳转为当前时间
        LocalDate localDate1 = Instant.ofEpochSecond(seconds).atZone(ZoneOffset.ofHours(8)).toLocalDate();
        // 2020-02-03
        System.out.println(localDate1);*/


        /*DateTimeFormatter dateTimeFormatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long milliseconds = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();

        System.out.println(milliseconds);
        System.out.println(LocalDateTime.now());
        String dateString = dateTimeFormatter2.format(LocalDateTime.now());
        System.out.println("日期转字符串: " + dateString);*/
       /* long milliseconds = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        final LocalDate localDate = Instant.ofEpochMilli(milliseconds).atZone(ZoneOffset.ofHours(8)).toLocalDate();

        final LocalTime localTime = Instant.ofEpochMilli(milliseconds).atZone(ZoneOffset.ofHours(8)).toLocalTime();
        final LocalDateTime localDateTime = Instant.ofEpochMilli(milliseconds).atZone(ZoneOffset.ofHours(8)).toLocalDateTime();
        System.out.println(localDateTime);*/

        /*Long timeStamp = System.currentTimeMillis();  //获取当前时间戳
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sd = sdf.format(timeStamp);
        System.out.println(sd);*/

       /* DateTimeFormatter strToDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        TemporalAccessor dateTemporal = strToDateFormatter.parse("2017-01-01 13:00:00");
        LocalDate date = LocalDate.from(dateTemporal);
        System.out.println(date);
        LocalDateTime dateTime = LocalDateTime.parse("2017-01-01 13:00:00", strToDateFormatter);
        System.out.println(dateTime.toString());*/

        //DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        //System.out.println(ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(1599189124891L), ZoneId.systemDefault())));

        /*DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime time = LocalDateTime.now();
        String localTime = df.format(time);
        System.out.println(localTime);*/

        DateTimeFormatter strToDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        TemporalAccessor dateTemporal = strToDateFormatter.parse("2017-01-01 01:00:00");
        LocalDate date = LocalDate.from(dateTemporal);
        System.out.println(date);


    }
}
