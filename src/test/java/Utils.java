import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;

public class Utils {

    private static Configuration conf = new Configuration(true);//读取配置文件

    static {
        conf.set("mapred.jar", "E:\\code\\ssy\\mapreduce\\out\\artifacts\\MyWC\\MyWC.jar");
        conf.set("fs.defaultFS", "hdfs://nn1.hadoop.bigdata.dmp.com:8020");
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    @Test
    public void aggregate() throws IOException {
        Path input = new Path( "/tmp/MR/prefix" );   //输出路径
        FileSystem fileSystem = input.getFileSystem(conf);
        if( !fileSystem.exists(input) ) {       //如果路径存在,删除路径
//            return false;
            System.out.println("路径不存在");
        }
        RemoteIterator<LocatedFileStatus> remoteIterator =
                fileSystem.listFiles(input, false);
        StringBuffer txtContent = new StringBuffer();
        while ( remoteIterator.hasNext() ) {
            LocatedFileStatus locatedFileStatus = remoteIterator.next();
            FSDataInputStream inputStream = fileSystem.open(locatedFileStatus.getPath());
            byte[] b = new byte[1];
            while (inputStream.read(b) != -1) {
                txtContent.append(new String(b));
            }
            inputStream.close();
        }
        System.out.println(txtContent.toString());


//        return true;
    }

    @Test
    public void readHdfsFile() throws IOException {
        Path input = new Path( "/tmp/MR/prefix/part-r-00004" );
        FileSystem fileSystem = input.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(input);
        byte[] b = new byte[1];
        StringBuffer txtContent = new StringBuffer();
        while (inputStream.read(b) != -1)
        {
            txtContent.append(new String(b));
        }
        System.out.println( txtContent.toString() );
        String str = txtContent.toString();
        String[] split = str.split("\t");
        String appKey = split[0];
        String count = split[1];
        System.out.println( "appkey:" + appKey );
        System.out.println( "count:" + count );

        inputStream.close();
    }
}
