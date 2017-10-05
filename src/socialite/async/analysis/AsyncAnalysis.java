package socialite.async.analysis;

import socialite.codegen.Analysis;
import socialite.parser.*;
import socialite.util.Assert;
import socialite.util.SociaLiteException;

import java.util.*;
import java.util.stream.Collectors;

public class AsyncAnalysis {
    private List<MyVariable> srcV;
    private List<MyVariable> dstV;
    private MyVariable extraV;
    /* 附加表 */
    /* 求值表达式 */
    private String sExpr;
    private Analysis an;
    private List<Rule> recRules;
    private MyVariable weightV;
    private String valueType;//value字段类型
    private String deltaType;//delta字段类型，一般valueType==deltaType，但2-step，valueType为boolean
    private String aggrName;
    private int initSize;
    /*  edge属性  */
    private Predicate edgeP;
    /* 求值表达式 */
    private Map<String, Table> tableMap;
    private AggrFunction aggrFunc;
    private Rule resultRule;//用于存储最终结果的rule，对于1-step，recRule和resultRule是同一个；对于2-step例子，ruleHead出现过两次的，把这个rule当作resultRule，另一条当作midRule
    private Rule midRule;//2-step用于计算中间结果的rule

    public AsyncAnalysis(Analysis analysis) {
        this.an = analysis;
        srcV = new ArrayList<>();
        dstV = new ArrayList<>();
        recRules = new ArrayList<>();
    }

    public boolean analysis() {
        if (init()) {
            getAggrFiledType();
            checkKeyType();
            findEdgePredicate();
            findExpr();
            return true;
        }
        return false;
    }

    public void addRecRule(Rule rule) {
        recRules.add(rule);
    }

    private boolean init() {
        tableMap = an.getTableMap();
        if (recRules.size() == 0)//无递归规则
            throw new SociaLiteException("recursive rule not found");
        if (recRules.size() == 1) {
            resultRule = recRules.get(0);//假定只有一个递归规则
        } else if (recRules.size() == 2) {//2-step example
            for (Rule recRule : recRules) {
                for (Rule rule : an.getRules()) {
                    if (recRule.getHead().name().equals(rule.getHead().name())) {//规则头部在其他规则中（初始规则）出现，该规则是resultRule
                        resultRule = recRule;
                        break;
                    }
                }
            }
            if (recRules.get(0) == resultRule)
                midRule = recRules.get(1);
            else midRule = recRules.get(0);
        }
        if (tableMap.get(resultRule.getHead().name()).isArrayTable())
            initSize = tableMap.get(resultRule.getHead().name()).arrayTableSize();
        else
            Assert.die("Need to be ArrayTable, Please specify a range.");
        return true;
    }

    private void getAggrFiledType() {
        Table aggrTable = null;
        for (Rule rule : recRules) {
            aggrFunc = rule.getHead().getAggrF();
            if (aggrFunc == null) throw new SociaLiteException("aggregate funtion not found");
            if (Arrays.stream(new String[]{"Builtin.dmin", "Builtin.dmax", "Builtin.dsum", "Builtin.dcount"}).noneMatch(funcName -> funcName.equals(aggrFunc.name())))
                throw new SociaLiteException("unsupported aggregate function " + aggrFunc.name());
            aggrTable = an.getTableMap().get(rule.getHead().name());
            break;
        }

        if (aggrFunc == null || aggrTable == null)
            throw new SociaLiteException("can not found aggregated function in rule head");
        aggrName = aggrFunc.name().split("\\.")[1];
        List<Param> args = aggrFunc.getArgs();
        if (args.size() != 1)
            Assert.die("only support 1 arg");

        deltaType = aggrTable.getColumn(aggrFunc.getIdx()).getType();
        if (resultRule.getHeadVariables().size() == 1) {//result表只有一列，则value为boolean
            valueType = boolean.class.toString();
        } else {
            valueType = deltaType;
        }
    }

    private void checkKeyType() {
        int aggFuncPos = aggrFunc.getIdx();

        Table table = tableMap.get(resultRule.getHead().name());
        if (aggFuncPos == 1) { //primitive key
            if (resultRule.getHead().getVariables().size() == 1) {//2-step
                srcV.add(new MyVariable(((Variable) resultRule.getHead().first()).name, table.getColumn(0).type()));
                table = tableMap.get(midRule.getHead().name());//中间结果表的第一列作为dst变量
                dstV.add(new MyVariable(((Variable) midRule.getHead().first()).name, table.getColumn(0).type()));
            } else if (resultRule.getHead().getVariables().size() == 2) {//1-step
                srcV.add(new MyVariable(((Variable) resultRule.firstP().first()).name, table.getColumn(0).type()));
                dstV.add(new MyVariable(((Variable) resultRule.getHead().first()).name, table.getColumn(0).type()));
            }
        } else if (aggFuncPos == 2) { //pair key
            srcV.add(new MyVariable(((Variable) resultRule.firstP().params.get(0)).name, table.getColumn(0).type()));
            srcV.add(new MyVariable(((Variable) resultRule.firstP().params.get(1)).name, table.getColumn(1).type()));
            dstV.add(new MyVariable(((Variable) resultRule.getHead().params.get(0)).name, table.getColumn(0).type()));
            dstV.add(new MyVariable(((Variable) resultRule.getHead().params.get(1)).name, table.getColumn(1).type()));
        } else {
            Assert.not_supported("support 1 or 2 field to compose key");
        }
    }

    private void findEdgePredicate() {
        Set<String> varSet = new HashSet<>();
        varSet.addAll(srcV.stream().map(MyVariable::getName).collect(Collectors.toSet()));
        varSet.addAll(dstV.stream().map(MyVariable::getName).collect(Collectors.toSet()));

        outer:
        for (Rule recRule : recRules) {
            for (Predicate predicate : recRule.getBodyP()) {
                Set<String> edgeSet = predicate.getVariables().stream().map(x -> x.name).collect(Collectors.toSet());
                Set<String> tmp = new HashSet<>(varSet);
                tmp.retainAll(edgeSet);
                if (tmp.size() == 2) {//edge谓词
                    edgeP = predicate;
                    break outer;
                }
            }
        }
        if (edgeP == null)
            throw new SociaLiteException("edge predicate not found");
        if (edgeP.params.size() == 3) {//edge 谓词有3列
            Table edgeTbl = an.getTableMap().get(edgeP.name());
            Variable weightVar = (Variable) edgeP.params.get(2);
            weightV = new MyVariable(weightVar.name, edgeTbl.getColumn(2).type());
        }
    }

    private void findExpr() {
        Expr expr = null;
        for (Rule recRule : recRules) {
            List<Expr> exprList = recRule.getExprs();
            if (exprList.size() > 0) {
                expr = exprList.get(0);
                break;
            }
        }
        if (expr == null) {
            throw new SociaLiteException("can not found expression in rule body");
        }

        List<Predicate> bodyPList = resultRule.getBodyP();
        Predicate headP = resultRule.getHead();
        Set<Variable> varInExpr = expr.getVariables();
        //bodyP过滤掉recP、剩下的谓词extraP需要满足：谓词的变量在求值表达式出现过，并且该谓词只有src变量没有dst变量（否则就是edgeP了）
        Predicate extraP = bodyPList.stream().filter(predicate -> {
            if (predicate.name().equals(headP.name()))//跳过recursive predicate
                return false;
            if (predicate == edgeP)//跳过edge
                return false;
            Set<Variable> varsInP = predicate.getVariables();
            varsInP.retainAll(varInExpr);
            return varsInP.size() > 0;
        }).findFirst().orElse(null);

        if (srcV.size() == 1) {
            if (extraP != null) {
                Column extraC = an.getTableMap().get(extraP.name()).getColumns()[1];//先假定第一列和key同名，第二列存值
                extraV = new MyVariable(((Variable) extraP.params.get(1)).name, extraC.type());//bug is here
            }
        } else {
            //对于顶点对程序，寻找extra变量
            //Assert.not_implemented();
            // program 5
        }

        if (expr.root instanceof AssignOp) {
            AssignOp assignOp = (AssignOp) expr.root;
            sExpr = assignExprToString(assignOp.arg2);
        } else if (expr.root instanceof CmpOp) {
            CmpOp cmpOp = (CmpOp) expr.root;
            Variable left = (Variable) cmpOp.getLHS();
            if (left.type.toString().equals(deltaType)) {
                sExpr = "oldDelta" + cmpOp.getOp() + cmpExprToString(cmpOp.getRHS());
            } else {
                Assert.impossible();
            }
        }
    }

    /**
     * 将表达式变量转换为AsyncTableSingle.eval所提供的变量。
     *
     * @param param
     * @return
     */
    private String assignExprToString(Object param) {
        if (param instanceof Variable) {
            Variable variable = (Variable) param;
            if (resultRule.firstP().getVariables().contains(variable))
                return "oldDelta";
            if (extraV != null && variable.name.equals(extraV.getName()))
                return "extra";
            if (weightV != null && variable.name.equals(weightV.getName()))
                return "weight";
            throw new SociaLiteException("unknown var: " + variable.name);
        } else if (param instanceof Const) {
            Const cst = (Const) param;
            return cst.constValStr();
        }
        BinOp binOp = (BinOp) param;
        return assignExprToString(binOp.arg1) + binOp.op + assignExprToString(binOp.arg2);
    }

    private String cmpExprToString(Object param) {
        if (param instanceof Variable) {
            Assert.not_implemented();
        } else if (param instanceof Const) {
            Const cst = (Const) param;
            return cst.constValStr();
        }
        BinOp binOp = (BinOp) param;
        return cmpExprToString(binOp.arg1) + binOp.op + cmpExprToString(binOp.arg2);
    }

    public String getKeyType() {
        assert srcV.size() == dstV.size();
        if (srcV.size() == 1) {
            return srcV.get(0).getType().toString();
        } else {
            return "Pair";
        }
    }

    public String getWeightType() {
        if (weightV == null)
            return null;
        return weightV.getType().toString();
    }

    public String getExtraType() {
        if (extraV == null)
            return null;
        return extraV.getType().toString();
    }

    public String getsExpr() {
        return sExpr;
    }

    public int getInitSize() {
        return initSize;
    }

    public String getResultPName() {
        return resultRule.getHead().name();
    }

    public String getValueType() {
        return valueType;
    }

    public String getAggrName() {
        return aggrName;
    }

    public boolean isTwoStep() {
        return midRule != null;
    }

    public String getInitRuleBody() {
        Rule rule = recRules.get(0);
        StringJoiner joiner = new StringJoiner(", ");
        for (Predicate predicate : rule.getBodyP())
            joiner.add(predicate.toString());
        return joiner.toString();
    }
}
