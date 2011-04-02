package markov;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import markov.MarkovProto.Chain;
import markov.MarkovProto.Tuple;
import me.prettyprint.hector.api.beans.HColumn;

public class TupleWriter {
    private final CF<Chain> chainCf;
    private final CF<Tuple> tupleCf;
    private final RIdIterInfo rIdIterInfo;
    private final StringIterInfo stringIterInfo;

    @Inject
    public TupleWriter(final CF<Chain> chainCf, final CF<Tuple> tupleCf, final RIdIterInfo rIdIterInfo,
            final StringIterInfo stringIterInfo) {
        this.chainCf = chainCf;
        this.tupleCf = tupleCf;
        this.rIdIterInfo = rIdIterInfo;
        this.stringIterInfo = stringIterInfo;
    }

    public void updateTuples(Set<Chain> chains) {
        for (final Chain chain : chains) {
            updateFromChain(chain);
        }
        tupleCf.flush();
    }

    private void updateFromChain(final Chain chain) {
        long count = 0;
        long start = 0;
        long end = 0;
        for (final HColumn<RId, byte[]> column : chainCf.forKey(chain).iter(rIdIterInfo, byte[].class)) {
            count += 1;
            final byte flags = column.getValue()[0];
            if ((flags & 1) != 0) {
                start += 1;
            }
            if ((flags & 2) != 0) {
                end += 1;
            }
        }
        final List<String> partList = chain.getPartList();
        {
            final int lastPart = partList.size() - 1;
            final Tuple forwardTuple = Tuple.newBuilder().setDirection(Tuple.Direction.FORWARD)
                    .addAllPart(partList.subList(0, lastPart)).build();
            tupleCf.forKey(forwardTuple).add(partList.get(lastPart), new long[] { count, end });
            updateTuple(forwardTuple);
        }
        {
            final Tuple backwardTuple = Tuple.newBuilder().setDirection(Tuple.Direction.BACKWARD)
                    .addAllPart(partList.subList(1, partList.size())).build();
            tupleCf.forKey(backwardTuple).add(partList.get(0), new long[] { count, start });
            updateTuple(backwardTuple);
        }
    }

    public void updateTuple(final Tuple tuple) {
        long total = 0;
        for (final HColumn<String, long[]> column : tupleCf.forKey(tuple).iter(stringIterInfo, long[].class)) {
            if ("".equals(column.getName())) {
                continue;
            }
            total += column.getValue()[0];
        }
        tupleCf.forKey(tuple).add("", new long[] { total });
    }
}
