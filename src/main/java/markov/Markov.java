package markov;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import markov.MarkovProto.Chain;
import markov.MarkovProto.Tuple;
import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class Markov {
    private final Splitter splitter = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();
    private final ChainWriter chainWriter;
    private final TupleWriter tupleWriter;

    @Inject
    public Markov(final ChainWriter chainWriter, final TupleWriter tupleWriter) {
        this.chainWriter = chainWriter;
        this.tupleWriter = tupleWriter;
    }

    private static class NullSerializer extends AbstractSerializer<Object> {
        public static final Object NULL = new Object();
        private static final NullSerializer instance = new NullSerializer();
        private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);

        public static NullSerializer get() {
            return instance;
        }

        @Override
        public ByteBuffer toByteBuffer(Object obj) {
            return ZERO_BUFFER;
        }

        @Override
        public Object fromByteBuffer(ByteBuffer byteBuffer) {
            return NULL;
        }
    }

    public void addString(final String string) {
        final List<String> pieces = Lists.newArrayList(splitter.split(string));
        final Entry<Set<Chain>, Set<UUID>> writeChains = chainWriter.txnWriter(pieces);
        tupleWriter.updateTuples(writeChains.getKey());
    }
}
