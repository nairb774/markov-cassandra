package markov;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import markov.MarkovProto.Chain;
import markov.MarkovProto.Tuple;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import com.google.common.base.Function;

public class TupleWriter {
    private final UUID minUuid = new UUID(Long.MIN_VALUE, Long.MIN_VALUE);
    private final UUID maxUuid = new UUID(Long.MAX_VALUE, Long.MAX_VALUE);
    private final Function<UUID, UUID> nextUuidFunction = new Function<UUID, UUID>() {
        @Override
        public UUID apply(UUID input) {
            long least = input.getLeastSignificantBits();
            long most = input.getMostSignificantBits();
            if (least == Long.MAX_VALUE) {
                if (most == Long.MAX_VALUE) {
                    return null;
                }
                least = Long.MIN_VALUE;
                most += 1;
            } else {
                least += 1;
            }
            return new UUID(most, least);
        }
    };
    private final CF<Chain> chainCf;
    private final CF<Tuple> tupleCf;

    @Inject
    public TupleWriter(final CF<Chain> chainCf, final CF<Tuple> tupleCf) {
        this.chainCf = chainCf;
        this.tupleCf = tupleCf;
    }

    public void updateTuples(Set<Chain> chains) {
        for (final Chain chain : chains) {
            updateTuple(chain);
        }
        tupleCf.flush();
    }

    private void updateTuple(final Chain chain) {
        int count = 0;
        int start = 0;
        int end = 0;
        for (final HColumn<UUID, byte[]> column : chainCf.forKey(chain).iter(minUuid, maxUuid, nextUuidFunction,
                byte[].class)) {
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
            tupleCf.forKey(forwardTuple).add(partList.get(lastPart), new int[] { count, end });
        }
        {
            final Tuple backwardTuple = Tuple.newBuilder().setDirection(Tuple.Direction.BACKWARD)
                    .addAllPart(partList.subList(1, partList.size())).build();
            tupleCf.forKey(backwardTuple).add(partList.get(0), new int[] { count, start });
        }
    }
}
