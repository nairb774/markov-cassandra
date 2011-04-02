package markov;

import javax.inject.Singleton;

import markov.CF.TypeMap;
import markov.MarkovProto.Chain;
import markov.MarkovProto.Source;
import markov.MarkovProto.Tuple;
import markov.ProtoSerializer.ChainProtoSerializer;
import markov.ProtoSerializer.SourceProtoSerializer;
import markov.ProtoSerializer.TupleProtoSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

public class MarkovModule extends AbstractModule {
    @Override
    protected void configure() {
        final Multibinder<Serializer<?>> serializers = Multibinder.newSetBinder(binder(),
                new TypeLiteral<Serializer<?>>() {
                });
        serializers.addBinding().to(BooleanSerializer.class);
        serializers.addBinding().to(ByteBufferSerializer.class);
        serializers.addBinding().to(BytesArraySerializer.class);
        serializers.addBinding().to(DateSerializer.class);
        serializers.addBinding().to(DoubleSerializer.class);
        serializers.addBinding().to(IntegerSerializer.class);
        serializers.addBinding().to(LongSerializer.class);
        serializers.addBinding().to(Utf8Serializer.class);
        serializers.addBinding().to(UUIDSerializer.class);

        serializers.addBinding().to(ChainProtoSerializer.class);
        serializers.addBinding().to(SourceProtoSerializer.class);
        serializers.addBinding().to(TupleProtoSerializer.class);
        serializers.addBinding().to(LongArraySerializer.class);
        serializers.addBinding().to(RIdSerializer.class);
    }

    @Provides
    public RIdFactory getRIdFactory() {
        return RIdFactory.get();
    }
    
    @Provides
    @Singleton
    public Cluster getCluster() {
        return HFactory.createCluster("Test Cluster", new CassandraHostConfigurator("127.0.0.1"));
    }

    @Provides
    public Keyspace getKeyspace(final Cluster cluster) {
        return HFactory.createKeyspace("main", cluster);
    }

    @Provides
    public CF<Chain> getChainCf(final Keyspace keyspace, final TypeMap typeMap) {
        return new CF<Chain>(keyspace, Chain.class, typeMap);
    }
    
    @Provides
    public CF<Source> getSourceCf(final Keyspace keyspace, final TypeMap typeMap) {
        return new CF<Source>(keyspace, Source.class, typeMap);
    }

    @Provides
    public CF<Tuple> getTupleCf(final Keyspace keyspace, final TypeMap typeMap) {
        return new CF<Tuple>(keyspace, Tuple.class, typeMap);
    }
}
