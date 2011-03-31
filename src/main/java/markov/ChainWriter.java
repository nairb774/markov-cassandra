package markov;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import markov.MarkovProto.Chain;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ChainWriter {
    private final CF<Chain> chainCf;
    private final int chainSize = 4;

    @Inject
    public ChainWriter(final CF<Chain> chainCf) {
        this.chainCf = chainCf;
    }

    public Entry<Set<Chain>, Set<UUID>> txnWriter(final List<String> pieces) {
        final int chains = pieces.size() - chainSize;
        final Set<Chain> chainSet = Sets.newHashSet();
        final Set<UUID> uuidSet = Sets.newHashSet();

        for (int i = 0; i <= chains; i++) {
            final Chain chain = Chain.newBuilder().addAllPart(pieces.subList(i, i + chainSize)).build();

            byte flag = 0;
            if (i == 0) {
                flag |= 1;
            }
            if (i == chains) {
                flag |= 2;
            }

            final UUID uuid = UUID.randomUUID();
            chainCf.forKey(chain).add(uuid, new byte[] { flag });

            chainSet.add(chain);
            uuidSet.add(uuid);
        }
        chainCf.flush();

        return Maps.immutableEntry(chainSet, uuidSet);
    }
}
