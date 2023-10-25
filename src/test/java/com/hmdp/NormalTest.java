package com.hmdp;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class NormalTest {

    @Test
    void testBitMap() {
        int i = 0b1110111111111111111111111;

        long t1 = System.nanoTime();
        int count = 0;
        while (true){
            if ((i & 1) == 0){
                    break;
            }else{
                count++;
            }
            i >>>= 1;
        }
        long t2 = System.nanoTime();
        System.out.println("time1 = " + (t2 - t1));
        System.out.println("count = " + count);

        i = 0b1110111111111111111111111;
        long t3 = System.nanoTime();
        int count2 = 0;
        while (true) {
            if(i >>> 1 << 1 == i){
                // 未签到，结束
                break;
            }else{
                // 说明签到了
                count2++;
            }

            i >>>= 1;
        }
        long t4 = System.nanoTime();
        System.out.println("time2 = " + (t4 - t3));
        System.out.println("count2 = " + count2);
    }

    @Test
    void test(){

        final Map<String, String> hashMap = new HashMap<>();
        hashMap.put("1", "1");
        for (int i = 0; i < 12; i++) {
            hashMap.put("" + i, null);
        }
        hashMap.put("1111", "111");
        hashMap.put(null, null);
        final String s = hashMap.get(null);
        hashMap.put(null, null);
        hashMap.put(null, null);
    }
}
