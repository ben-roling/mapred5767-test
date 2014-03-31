package org.example;

import java.io.Serializable;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

@SuppressWarnings("serial")
final class FirstStringMapFn extends MapFn<StringList, Pair<String, StringList>> implements Serializable {
    @Override
    public Pair<String, StringList> map(final StringList input) {
        return Pair.of(input.getValue().get(0).toString(), input);
    }
}