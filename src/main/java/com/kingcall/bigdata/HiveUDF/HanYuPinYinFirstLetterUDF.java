package com.kingcall.bigdata.HiveUDF;


import net.sourceforge.pinyin4j.PinyinHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Description(name = "hanyupinyin", value = "_FUNC_(" +
        "String input 汉语(你)\n" +
        ") - return result(n)",
        extended = ""
)
/**
 * 首个字符是汉语则返回拼音首字母
 * 否则返回第一个字符 数字或者字母
 */
public class HanYuPinYinFirstLetterUDF extends UDF {
    private static Map<String, String> pinyinMap = new HashMap<String, String>();

    static {
        initPinyin("/duoyinzi_dic.txt");
    }

    // 空则返回 空字符串
    public String evaluate(String input) {
        if (StringUtils.isBlank(input) || input == null) {
            return "";
        }
        // 不是汉语 则返回第一个字符
        if (!isChinese(input)){
            return input.substring(0, 1);
        }

        // 是汉语 先判断是不是多音字
        if (pinyinMap.get(input) != null) {
            return pinyinMap.get(input).substring(0, 1);
        } else {
            // 不是多音字直接处理
            char aChar = input.toCharArray()[0];
            String result = PinyinHelper.toHanyuPinyinStringArray(aChar)[0];
            return result.substring(0, 1);
        }
    }

    /**
     * 检查输入是否为字符串
     *
     * @param input
     * @return
     */
    public static boolean isChinese(String input) {
        input = input.substring(0, 1);
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(input);
        if (m.find()) {
            return true;
        }
        return false;

    }


    /**
     * 初始化 所有的多音字词组
     *
     * @param fileName
     */

    public static void initPinyin(String fileName) {
        // 读取多音字的全部拼音表;
        InputStream file = PinyinHelper.class.getResourceAsStream(fileName);

        BufferedReader br = new BufferedReader(new InputStreamReader(file));

        String s = null;
        try {
            while ((s = br.readLine()) != null) {

                if (s != null) {
                    String[] arr = s.split("#");
                    String pinyin = arr[0];
                    String chinese = arr[1];

                    if (chinese != null) {
                        String[] strs = chinese.split(" ");
                        for (String str : strs) {
                            pinyinMap.put(str, pinyin);
                        }

                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPublicOpinionAnalysisUDF() throws HiveException {
        HanYuPinYinFirstLetterUDF udf = new HanYuPinYinFirstLetterUDF();
        String res;
        res = udf.evaluate("长安");
        System.out.println(res);
        res = udf.evaluate("长大");
        System.out.println(res);
        res = udf.evaluate("你好");
        System.out.println(res);
        res = udf.evaluate("ab");
        System.out.println(res);
    }
}
