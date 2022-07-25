package com.imooc.data.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Java代码操作HDFS
 * Created by xuwei
 */
public class HdfsOpUtil {

    /**
     * 上传文件
     * @throws IOException
     */
    public static void put(String inContent,String outPath,String fileName) throws IOException {
        //创建一个配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        //获取操作HDFS的对象
        FileSystem fileSystem = FileSystem.get(conf);
        Path f = new Path(outPath);
        //先尝试删除，再创建
        fileSystem.delete(f,true);
        fileSystem.mkdirs(f);

        //获取输入流
        ByteArrayInputStream fis = new ByteArrayInputStream(inContent.getBytes());
        //获取HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.create(new Path(outPath+"/"+fileName));
        //上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis,fos,1024,true);
    }

}
