package socialite.async;

import socialite.util.Assert;
import socialite.util.SociaLiteException;

public class AsyncConfig {
    private static AsyncConfig asyncConfig;
    private int checkInterval = -1;
    private double threshold;
    private CheckerType checkType;
    private Cond cond;
    private boolean dynamic;
    private boolean debugging;

    public static AsyncConfig get() {
        if (asyncConfig == null) {
            throw new SociaLiteException("AsyncConfig is not create");
        }
        return asyncConfig;
    }

    public int getCheckInterval() {
        return checkInterval;
    }

    public double getThreshold() {
        return threshold;
    }

    public CheckerType getCheckType() {
        return checkType;
    }

    public String getCond() {
        switch (cond) {
            case G:
                return ">";
            case GE:
                return ">=";
            case E:
                return "==";
            case LE:
                return "<=";
            case L:
                return "<";
        }
        Assert.impossible();
        return null;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public boolean isDebugging() {
        return debugging;
    }

    public enum Cond {
        G, GE, E, L, LE
    }

    public enum CheckerType {
        VALUE, DELTA
    }

    public static class Builder {
        private int checkInterval = -1;
        private Double threshold = null;
        private CheckerType checkType;
        private Cond cond;
        private boolean dynamic;
        private boolean debugging;

        public Builder setCheckerType(CheckerType checkType) {
            this.checkType = checkType;
            return this;
        }

        public Builder setCheckInterval(int checkInterval) {
            this.checkInterval = checkInterval;
            return this;
        }

        public Builder setThreshold(double threshold) {
            this.threshold = threshold;
            return this;
        }

        public Builder setCheckerCond(Cond cond) {
            this.cond = cond;
            return this;
        }

        public Builder setDynamic(boolean dynamic){
            this.dynamic = dynamic;
            return this;
        }

        public Builder setDebugging(boolean debugging){
            this.debugging = debugging;
            return this;
        }

        public AsyncConfig build() {
            AsyncConfig asyncConfig = new AsyncConfig();
            if (threshold == null)
                throw new SociaLiteException("threshold is not set");
            if (checkType == null)
                throw new SociaLiteException("check type is not set");
            if (cond == null)
                throw new SociaLiteException("condition is not set");
            asyncConfig.checkInterval = checkInterval;
            asyncConfig.threshold = threshold;
            asyncConfig.checkType = checkType;
            asyncConfig.cond = cond;
            asyncConfig.dynamic = dynamic;
            asyncConfig.debugging = debugging;
            if(AsyncConfig.asyncConfig!=null)
                throw new SociaLiteException("AsyncConfig already built");
            AsyncConfig.asyncConfig = asyncConfig;
            return asyncConfig;
        }
    }
}
