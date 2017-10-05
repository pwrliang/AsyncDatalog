package socialite.async.codegen;

import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;

import java.util.ArrayList;
import java.util.List;

public class DistAsyncCodeGen {
    private AsyncAnalysis asyncAn;
//    private AsyncConfig asyncConfig = AsyncConfig.get();

    public DistAsyncCodeGen(AsyncAnalysis asyncAn) {
        this.asyncAn = asyncAn;
    }

    public List<String> generateInitStat() {
        List<String> initStats=new ArrayList<>();
        if (asyncAn.getKeyType().equals("Pair")) {

        } else {
            if (asyncAn.isTwoStep()) {

            } else {
                //xxx_mid(src, delta, (dst, weight))
                //xxx_mid(src, delta, extra, (dst))
                //
//                if (asyncAn.getExtra() == null && asyncAn.getWeightV() != null) {//sssp
//                    //                                  key     delta     dst   weight
//                    initStats.add(String.format("%s_mid(%s %s:0..%d, %s delta, (%s %s, %s %s)).",
//                            asyncAn.getClassName(),
//                            asyncAn.getSrcV().get(0).getType(), asyncAn.getSrcV().get(0).getName(),
//                            asyncAn.getInitSize(),
//                            asyncAn.getDeltaType(),
//                            asyncAn.getDstV().get(0).getType(), asyncAn.getDstV().get(0).getName(),
//                            asyncAn.getWeightV().getType(), asyncAn.getWeightV().getName())
//                    );
//                    initStats.add(String.format("%s_mid(%s, %s, %s, %s) :- %s.",
//                            asyncAn.getClassName(),
//                            asyncAn.getResultRule().firstP().params.get(0),
//                            asyncAn.getResultRule().firstP().params.get(1),
//                            asyncAn.getDstV().get(0).getName(),
//                            asyncAn.getWeightV().getName(),
//                            asyncAn.getInitRuleBody()
//                            )
//
//                    );
//                } else if (asyncAn.getExtra() != null && asyncAn.getWeightV() == null) {//pagerank
//                    initStats.add( String.format("%s_mid(%s %s:0..%d, %s delta, %s %s, (%s %s)).",
//                            asyncAn.getClassName(),
//                            asyncAn.getSrcV().get(0).getType(), asyncAn.getSrcV().get(0).getName(),
//                            asyncAn.getInitSize(),
//                            asyncAn.getDeltaType(),
//                            asyncAn.getExtra().getType(), asyncAn.getExtra().getName(),
//                            asyncAn.getDstV().get(0).getType(), asyncAn.getDstV().get(0).getName())
//                    );
//                    //                                key delta extra dst rule body
//                    initStats.add(String.format("%s_mid(%s, %s, %s, %s) :- %s.",
//                            asyncAn.getClassName(),
//                            asyncAn.getResultRule().firstP().params.get(0), //key
//                            asyncAn.getResultRule().firstP().params.get(1), //delta
//                            asyncAn.getExtra().getName(),
//                            asyncAn.getDstV().get(0).getName(),
//                            asyncAn.getInitRuleBody())
//                    );
//                    System.out.println();
//                }
            }
        }
        return initStats;
    }
}
