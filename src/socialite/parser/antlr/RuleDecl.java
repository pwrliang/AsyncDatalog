package socialite.parser.antlr;

import socialite.collection.SArrayList;
import socialite.parser.Const;
import socialite.parser.Expr;
import socialite.parser.Literal;
import socialite.parser.Predicate;
import socialite.util.Assert;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

public class RuleDecl implements Externalizable {
    private static final long serialVersionUID = 1;

    public Predicate head;
    public SArrayList<Literal> body;

    boolean simpleUpdate = false;

    public RuleDecl() {
    }

    public RuleDecl(Predicate _head, List<Literal> _body) {
        head = _head;
        body = new SArrayList<Literal>(_body);
    }

    public String toString() {
        String result = head.toString() + " :- ";
        for (int i = 0; i < body.size(); i++) {
            Object o = body.get(i);
            boolean noComma = false;
            if (o instanceof Const) {
                Const c = (Const) o;
                result += c.constValStr();
            } else if (o instanceof Expr) {
                //process like this, (X<5)->X<5
                String expr = o.toString();
                int lParenth = 0, rParenth = 0;
                for (Character c : expr.toCharArray()) if (c == '(') lParenth++;
                for (Character c : expr.toCharArray()) if (c == ')') rParenth++;
                if (lParenth == 1 && rParenth == 1 && expr.startsWith("(") && expr.endsWith(")"))
                    result += expr.substring(1, expr.length() - 1);
                else result += expr;
            } else {
                result += o;
                if (("" + o).length() == 0)
                    noComma = true;
            }
            if (i != body.size() - 1 && !noComma) result += ",";
        }
        result += ".";
        return result;
    }

    public boolean isSimpleUpdate() {
        return simpleUpdate;
    }

    public void setAnnotation(String ann) {
        if ("SimpleUpdate".equalsIgnoreCase(ann)) {
            simpleUpdate = true;
        } else {
            Assert.die("Unsupported annotation:" + ann);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        head = (Predicate) in.readObject();
        body = new SArrayList<Literal>(0);
        body.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(head);
        body.writeExternal(out);
    }
}
