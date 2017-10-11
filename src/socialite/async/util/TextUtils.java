package socialite.async.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class TextUtils {
    public static void writeText(String path, String code) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            writer.write(code);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String readText(String path) {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            StringBuilder sb = new StringBuilder();
            String program;
            while ((program = reader.readLine()) != null)
                sb.append(program).append('\n');
            program = sb.toString();
            return program;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private BufferedWriter writer;
    private FileSystem hdfs;

    public TextUtils(String hdfsOrLocalPath, String fileName) {
        try {
            if (hdfsOrLocalPath.toLowerCase().startsWith("hdfs://")) {
                Configuration conf = new Configuration();
                Path rootDir = new Path(hdfsOrLocalPath);
                hdfs = FileSystem.get(rootDir.toUri(), conf);
                if (!hdfs.exists(rootDir))
                    if (!hdfs.mkdirs(rootDir))
                        throw new RuntimeException("create folder failed");
                hdfs.close();
                Path textFile = new Path(hdfsOrLocalPath + "/" + fileName);
                hdfs = FileSystem.get(textFile.toUri(), conf);
                writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(textFile), "UTF-8"));
            } else {
                File rootDir = new File(hdfsOrLocalPath);
                if (!rootDir.exists())
                    if (!rootDir.mkdirs())
                        throw new RuntimeException("create folder failed");
                writer = new BufferedWriter(new FileWriter(hdfsOrLocalPath + "/" + fileName));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeLine(String line) {
        try {
            writer.write(line);
            writer.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            writer.close();
            if (hdfs != null) hdfs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
