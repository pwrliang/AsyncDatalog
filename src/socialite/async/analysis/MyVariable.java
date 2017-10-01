package socialite.async.analysis;

import socialite.parser.Variable;

public class MyVariable extends Variable {
    private String name;
    private Class type;

    public MyVariable(String name, Class type) {
        this.name = name;
        this.type = type;
    }

    public MyVariable(Variable variable) {
        name = variable.name;
        type = variable.type;
    }

    @Override
    public String toString() {
        return type + " " + name;
    }

    public String getName() {
        return name;
    }

    public Class getType() {
        return type;
    }

    @Override
    public void setType(Class type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        MyVariable that = (MyVariable) o;

        if (!name.equals(that.name)) return false;
        return type.equals(that.type);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
