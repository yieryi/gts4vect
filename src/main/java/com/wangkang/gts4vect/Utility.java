package com.wangkang.gts4vect;


import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.collection.CompositeCollection;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * @author wangkang
 * @email iwuang@qq.com
 * @date 2019/1/24 18:49
 */
public class Utility {
    private static Logger logger = Logger.getLogger(Utility.class);
    public Date start = null;

    //    读取判断
    public static boolean valiFileForRead(String filepath) {
        File file = new File(filepath);
        return file.exists();
    }

    //写入判断
    public static boolean valiFileForWrite(String filepath) {
        File file = new File(filepath);
        boolean result = false;
        if (file.exists()) {
            deleteDir(file);
        }
        try {
            result = file.createNewFile();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return result;
    }

    //    删除指定文件或者文件夹
    public static boolean deleteDir(final File dir) {
        if (dir.isDirectory()) {
            final String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                final boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

    //    判断空对象，判断空数组，判断空的字符串
    public static boolean isEmpty(Object o) {
        if (o == null) {
            return true;
        }
        if (o.getClass().equals(String.class) && String.valueOf(o).trim().length() < 1) {
            return true;
        }
        if (Collection.class.isAssignableFrom(o.getClass()) && ((Collection) (o)).size() < 1) {
            List list = new ArrayList();
            Collection collection = new CompositeCollection();
        }
        return false;
    }

    public static boolean valiShpField(String filedname) {
        boolean result = false;

        return result;
    }

    public  void startRecord(Object o) {
        if (!isEmpty(o)) {
            System.out.println(JSON.toJSONString(o));
        }
        start = new Date();
    }

    public  void tagLast(Object o) {
        Date date = new Date();
        System.out.println("\n========= "+JSON.toJSONString(o) + " --------> " + (date.getTime() - start.getTime() + "毫秒"));
        start = date;
    }
}
