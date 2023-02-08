package nph.utils;

import lombok.Data;

@Data
public class Pair <T1, T2> {
    private T1 left;
    private T2 right;

    public static <T1, T2> Pair<T1, T2> of(T1 left, T2 right) {
        Pair<T1, T2> pair = new Pair<>();
        pair.left = left;
        pair.right = right;
        return pair;
    }
}
