package io.pereira.iceberglookupsrv.algo;

public record BasicRange<T extends Comparable<T>>(T lowerValue, T upperValue) implements Range<T> {
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BasicRange that = (BasicRange) o;
        return lowerValue.equals(that.lowerValue) && upperValue.equals(that.upperValue);
    }

    @Override
    public String toString() {
        return "BasicRange{" +
                "lowerValue=" + lowerValue +
                ", upperValue=" + upperValue +
                '}';
    }
}
