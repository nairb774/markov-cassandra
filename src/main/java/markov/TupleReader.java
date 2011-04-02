package markov;

import javax.inject.Inject;

import markov.MarkovProto.Tuple;

public class TupleReader {
    private final CF<Tuple> tupleCf;

    @Inject
    public TupleReader(final CF<Tuple> tupleCf) {
        this.tupleCf = tupleCf;
    }

    
}
