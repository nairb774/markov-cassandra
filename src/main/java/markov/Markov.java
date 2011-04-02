package markov;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import markov.MarkovProto.Chain;
import markov.MarkovProto.Source;
import me.prettyprint.cassandra.serializers.AbstractSerializer;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class Markov {
    private final Splitter splitter = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();
    private final SourceWriter sourceWriter;
    private final ChainWriter chainWriter;
    private final TupleWriter tupleWriter;
    private final RIdFactory rIdFactory;

    @Inject
    public Markov(final SourceWriter sourceWriter, final ChainWriter chainWriter, final TupleWriter tupleWriter,
            final RIdFactory rIdFactory) {
        this.sourceWriter = sourceWriter;
        this.chainWriter = chainWriter;
        this.tupleWriter = tupleWriter;
        this.rIdFactory = rIdFactory;
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

    public void addString(final Source source, final String string) {
        final RId txnId = rIdFactory.next();
        sourceWriter.write(source, txnId, string);
        final List<String> pieces = Lists.newArrayList(splitter.split(string));
        final Set<Chain> writeChains = chainWriter.txnWriter(txnId, pieces);
        tupleWriter.updateTuples(writeChains);
    }
    
    public void lookup() {
        
    }
}
