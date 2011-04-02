package markov;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import markov.MarkovProto.Chain;

import com.google.common.collect.Sets;

public class ChainWriter {
    private final CF<Chain> chainCf;
    private final int chainSize = 4;

    @Inject
    public ChainWriter(final CF<Chain> chainCf) {
        this.chainCf = chainCf;
    }

    public Set<Chain> txnWriter(final RId txnId, final List<String> pieces) {
        final int chains = pieces.size() - chainSize;
        final Set<Chain> chainSet = Sets.newHashSet();

        final RIdFactory idFactory = new RIdFactory(txnId);
        for (int i = 0; i <= chains; i++) {
            final Chain chain = Chain.newBuilder().addAllPart(pieces.subList(i, i + chainSize)).build();

            byte flag = 0;
            if (i == 0) {
                flag |= 1;
            }
            if (i == chains) {
                flag |= 2;
            }

            final RId id = idFactory.next();
            chainCf.forKey(chain).add(id, new byte[] { flag });

            chainSet.add(chain);
        }
        chainCf.flush();

        return chainSet;
    }
}
