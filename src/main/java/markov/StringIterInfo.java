package markov;

import markov.CF.IterInfo;

public class StringIterInfo implements IterInfo<String> {
    @Override
    public String next(final String name) {
        return name + '\0';
    }
}
