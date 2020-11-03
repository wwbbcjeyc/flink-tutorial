package com.zjtd.other;

import java.util.Random;
import java.util.UUID;

public class UUIDTest {
    public static void main(String[] args) {
        String uId = UUID.randomUUID().toString();
        System.out.println(uId);
        System.out.println(uId.replace("-", ""));

        int machineId = 1;//最大支持1-9个集群机器部署
        int hashCodeV = UUID.randomUUID().toString().hashCode();
        System.out.println(hashCodeV);
        if(hashCodeV < 0) {//有可能是负数
            hashCodeV = - hashCodeV;
        }
        // 0 代表前面补充0
        // 4 代表长度为4
        // d 代表参数为正数型
        System.out.println(machineId+String.format("%015d", hashCodeV));

        System.out.println((int) (Math.random() * 4));
        System.out.println(Math.random());
        System.out.println(Math.random() * 100);
        System.out.println("##############");
        System.out.println(new Random().nextInt(10));
        System.out.println(Math.random() * 10);

    }


}
