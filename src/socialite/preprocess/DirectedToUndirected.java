package socialite.preprocess;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.*;

public class DirectedToUndirected {
    public static void main(String[] args) {
        TIntObjectMap<TIntSet> edgeMap = new TIntObjectHashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tmp = line.split("\\s+");
                int src = Integer.parseInt(tmp[0]);
                int dst = Integer.parseInt(tmp[1]);
                TIntSet dstSet = edgeMap.get(src);
                if (dstSet == null) {
                    dstSet = new TIntHashSet();
                    edgeMap.put(src, dstSet);
                }
                dstSet.add(dst);

                dstSet = edgeMap.get(dst);
                if (dstSet == null) {
                    dstSet = new TIntHashSet();
                    edgeMap.put(dst, dstSet);
                }
                dstSet.add(src);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(args[1]))) {
            edgeMap.forEachEntry((src, dstList) -> {
                dstList.forEach(dst -> {
                    try {
                        writer.write(String.format("%d\t%d\n", src, dst));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return true;
                });
                return true;
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}