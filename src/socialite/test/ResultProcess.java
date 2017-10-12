package socialite.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class ResultProcess {
    public static void main(String[] args) {

    }

    static void PageRank() throws IOException {
        Configuration conf = new Configuration();
        Path textFile = new Path("");
        FileSystem hdfs = FileSystem.get(textFile.toUri(), conf);
        BufferedReader reader= new BufferedReader(new InputStreamReader(hdfs.open(textFile), "UTF-8"));

    }
}
