package socialite.test;

import com.google.common.util.concurrent.AtomicDouble;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntDoubleHashMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

public class ReadAnswer {
    public static TIntDoubleMap PageRankAnswer = new TIntDoubleHashMap();
    public static final double THRESHOLD = 0.0001;

    public static void load() {
        //read spark saveasfile
        File file = new File("/home/gengl/Datasets/web-Google_fix.txt100");
//        File file = new File("/home/gengl/IdeaProjects/Datasets/LiveJournal-edge_fix_100");
//        File file = new File("/home/gengl/IdeaProjects/Datasets/web-BerkStan_fix.txt100");
        if (!file.exists() || file.list() == null || file.list().length == 0) {
            System.err.println("answer does not exists");
        } else {
            for (File dataFile : file.listFiles()) {
                try (Stream<String> reader = Files.lines(Paths.get(dataFile.getPath()))) {
                    reader.forEach(line -> {
                        line = line.replace('(', '\0').replace(')', '\0');
                        String[] t = line.split(",");
                        PageRankAnswer.put(Integer.parseInt(t[0].trim()), Double.parseDouble(t[1].trim()));
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static float compare(List<AtomicDouble> inputAnswer) {
        if (PageRankAnswer.size() == 0)
            return -1;
        if (inputAnswer.size() != PageRankAnswer.size()) {
            throw new RuntimeException("error answer");
        }
        double sum = 0;
        for (int ind = 0; ind < PageRankAnswer.size(); ind++) {
            sum += Math.abs(inputAnswer.get(ind).floatValue() - PageRankAnswer.get(ind));
        }
        return (float) sum;
    }

    public static void compare(TIntFloatMap inputAnswer) {
        if (PageRankAnswer.size() == 0)
            return;
        if (inputAnswer.size() != PageRankAnswer.size()) {
            throw new RuntimeException("error answer");
        }
        double sum = 0;
        for (int ind = 0; ind < PageRankAnswer.size(); ind++) {
            sum += Math.abs(inputAnswer.get(ind) - PageRankAnswer.get(ind));
        }
        System.out.println("SUM(R[i]-R*[i]) = " + sum);
        if (sum < THRESHOLD)
            System.out.println("sum < THRESHOLD");
    }

    public static void compare(TIntDoubleMap inputAnswer) {
        if (PageRankAnswer.size() == 0)
            return;
        if (inputAnswer.size() != PageRankAnswer.size()) {
            throw new RuntimeException("error answer");
        }
        double sum = 0;
        for (int ind = 0; ind < PageRankAnswer.size(); ind++) {
            sum += Math.abs(inputAnswer.get(ind) - PageRankAnswer.get(ind));
        }
        System.out.println("SUM(R[i]-R*[i]) = " + sum);
        if (sum < THRESHOLD)
            System.out.println("sum < THRESHOLD");
    }
}
