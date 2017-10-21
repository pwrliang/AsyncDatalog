package socialite.preprocess;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.*;
import java.util.Random;

public class FixEdgePair {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("usage: [input missing source edge pair graph] [output fixed edge pair graph] [splitter]");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        String splitter = args[2];
        if (splitter.equals("\\s") || splitter.equals("\\t"))
            splitter = "\\s+";

        TIntSet nodeSet = new TIntHashSet();
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(input));
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#") || line.startsWith("%")) continue;
            String[] tmp = line.split(splitter);
            int src = Integer.parseInt(tmp[0]);
            int dst = Integer.parseInt(tmp[1]);
            nodeSet.add(src);
            nodeSet.add(dst);
        }
        reader.close();

        System.out.println("loaded");

        TIntList nodeList = new TIntArrayList(nodeSet);
        nodeList.sort();
        TIntIntMap nodeReorderedMap = new TIntIntHashMap();
        int counter = 0;
        for (int ind = 0; ind < nodeList.size(); ind++) {
            nodeReorderedMap.put(nodeList.get(ind), counter++);
        }

        System.out.println("tagged");

        reader = new BufferedReader(new FileReader(input));
        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        Random random = new Random();

        int last = 0;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#") || line.startsWith("%")) continue;
            String[] tmp = line.split("\\s+");
            int src = Integer.parseInt(tmp[0]);
            int dst = Integer.parseInt(tmp[1]);
            int reorderedSrc = nodeReorderedMap.get(src);
            int reorderedDst = nodeReorderedMap.get(dst);

            if (reorderedSrc - last > 1)
                for (int fakeSrc = last + 1; fakeSrc < reorderedSrc; fakeSrc++) {
                    writer.write(String.format("%d\t%d\n", fakeSrc, random.nextInt(nodeSet.size())));
                }
            writer.write(String.format("%d\t%d\n", reorderedSrc, reorderedDst));
            last = reorderedSrc;
        }
        for (int fakeSrc = last + 1; fakeSrc < nodeSet.size(); fakeSrc++) {
            writer.write(String.format("%d\t%d\n", fakeSrc, random.nextInt(nodeSet.size())));
        }

        writer.close();
        reader.close();
    }
}
