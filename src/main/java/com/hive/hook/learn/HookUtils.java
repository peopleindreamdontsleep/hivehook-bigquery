package com.hive.hook.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class HookUtils {

    public static Configuration getHdfsConfig(int flag){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "xxxxxxx");
        conf.set("fs.defaultFS", "hdfs://HDFS43319/");
        return conf;
    }

    public static long getTotalSize(String pathString) throws IOException {
        FileSystem hdfs = FileSystem.get(HookUtils.getHdfsConfig(0));


        //Path fpath = new Path(pathString.replace("JD","114.67.103.201"));
        Path fpath = new Path(pathString);
        FileStatus[] fileStatuses = hdfs.listStatus(fpath);
        Path[] listPath = FileUtil.stat2Paths(fileStatuses);
        long totalSize=0L;
        for (Path eachpath:listPath) {
            //totalSize = totalSize+hdfs.getContentSummary(new Path(eachpath.toString().replace("JD","114.67.103.201"))).getLength();
            totalSize = totalSize+hdfs.getContentSummary(eachpath).getLength();
        }

        return totalSize;
    }

    public static long getListPathSize(ArrayList<String> inputList){
        long totalSize=0L;

        for (String eachPath:inputList) {
            try {
                totalSize = totalSize+getTotalSize(eachPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return totalSize;
    }

    public static String getValueName(Map<String, String> spec, boolean addTrailingSeperator) throws MetaException {
        StringBuilder suffixBuf = new StringBuilder();
        int i = 0;

        for(Iterator var4 = spec.entrySet().iterator(); var4.hasNext(); ++i) {
            Map.Entry<String, String> e = (Map.Entry)var4.next();
            if (e.getValue() == null || ((String)e.getValue()).length() == 0) {
                throw new MetaException("Partition spec is incorrect. " + spec);
            }

            if (i > 0) {
                suffixBuf.append("/");
            }
            suffixBuf.append(escapePathName((String)e.getValue()));
        }

        if (addTrailingSeperator) {
            suffixBuf.append("/");
        }

        return suffixBuf.toString();
    }

    static String escapePathName(String path) {
        return FileUtils.escapePathName(path);
    }


}
