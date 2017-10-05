package socialite.async.codegen;

public  class Key {
    int v0;
    int v1;
    private int hashCode = -1;

    Key(int v0, int v1) {
        this.v0 = v0;
        this.v1 = v1;
    }

    @Override
    public String toString() {
        return v0 + " " + v1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Key key = (Key) o;

        if (v0 != key.v0) return false;
        return v1 == key.v1;
    }

    @Override
    public int hashCode() {
        if (hashCode != -1)
            return hashCode;
        hashCode = v0;
        hashCode = 31 * hashCode + v1;
        return hashCode;
    }
}