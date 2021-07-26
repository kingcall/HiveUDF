package com.kingcall.bigdata.fileFormat;

import com.github.javafaker.*;
import com.github.javafaker.Number;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

class AccessLog {
    String track_time;
    String url;
    String session_id;
    String referer;
    String ip;
    String end_user_id;
    String city_id;

}

public class ProduceTestData {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd HH:MM:ss");

    @Test
    public void testRandomName() throws IOException {
        Faker faker = new Faker(Locale.CHINA);
        final Name name = faker.name();
        final Address address = faker.address();
        Number number = faker.number();
        PhoneNumber phoneNumber = faker.phoneNumber();

        BufferedWriter out = new BufferedWriter(new FileWriter("/Users/liuwenqiang/access.log"));
        int num=0;
        while (num<10000000){
            int id = number.randomDigitNotZero();
            String userName = name.name();
            String time = simpleDateFormat.format(new Date(System.currentTimeMillis()));
            String city = address.city();
            String phonenum = phoneNumber.cellPhone();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(id);
            stringBuilder.append("\t");

            stringBuilder.append(userName);
            stringBuilder.append("\t");

            stringBuilder.append(city);
            stringBuilder.append("\t");

            stringBuilder.append(phonenum);
            stringBuilder.append("\t");

            stringBuilder.append(time);

            out.write(stringBuilder.toString());
            out.newLine();
        }
        out.flush();
        out.close();
    }

}


