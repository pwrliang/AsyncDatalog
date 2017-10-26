package socialite.preprocess;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import socialite.async.codegen.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FindPathDAG {
    static class Node {
        void AddLink(int id) {
            next.add(id);
        }

        TIntList next = new TIntArrayList();
    }

    static void FindAllPathsAt(List<Node> all_nodes, int id, List<TIntList> all_paths, TIntList tmp) {
        tmp.add(id);

        if (all_nodes.get(id).next.size() == 0) {
            all_paths.add(tmp);
            return;
        }

        for (int i = 0; i < all_nodes.get(id).next.size(); i++) {
            TIntList tmp2 = new TIntArrayList(tmp);
            FindAllPathsAt(all_nodes, all_nodes.get(id).next.get(i), all_paths, tmp2);
        }
    }

    static TObjectIntMap<Pair> cpath = new TObjectIntHashMap<>();

    static void PrintPaths(List<TIntList> all_paths) {
        for (int i = 0; i < all_paths.size(); i++) {
            // Don't print node if it points to nothing
            if (all_paths.get(i).size() == 1) {
                continue;
            }

//            System.out.print();
//
//            for (int j = 1; j < all_paths.get(i).size(); j++) {
//                System.out.print(" -- > " + all_paths.get(i).get(j));
//            }

            Pair pair = new Pair(all_paths.get(i).get(0), all_paths.get(i).get(all_paths.get(i).size() - 1));
            if (cpath.containsKey(pair))
                cpath.put(pair, cpath.get(pair) + 1);
            else
                cpath.put(pair, 1);

//            System.out.println();
        }
    }

    public static void main(String[] args) throws IOException {
        List<Node> all_nodes = new ArrayList<>();
        for (int i = 0; i < 88; i++)
            all_nodes.add(new Node());
        BufferedReader reader = new BufferedReader(new FileReader("/home/gengl/Datasets/dag.txt"));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\s+");
            all_nodes.get(Integer.parseInt(tmp[0])).AddLink(Integer.parseInt(tmp[1]));
        }
        reader.close();
        TIntList tmp = new TIntArrayList(); // work space

        for (int i = 0; i < all_nodes.size(); i++) {
            List<TIntList> all_paths = new ArrayList<>();
            FindAllPathsAt(all_nodes, i, all_paths, tmp);

//            System.out.println("All paths at node " + i);
            PrintPaths(all_paths);
            tmp.clear();
        }
        for (Pair pair : cpath.keySet())
            System.out.println(pair + "\t" + cpath.get(pair));
    }
}
