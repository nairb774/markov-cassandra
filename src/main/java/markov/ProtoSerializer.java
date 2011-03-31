package markov;

import java.nio.ByteBuffer;

import markov.MarkovProto.Chain;
import markov.MarkovProto.Tuple;
import me.prettyprint.cassandra.serializers.AbstractSerializer;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

public class ProtoSerializer<T extends Message> extends AbstractSerializer<T> {
    public static class ChainProtoSerializer extends ProtoSerializer<Chain> {
        public ChainProtoSerializer() {
            super(Chain.getDefaultInstance());
        }
    }

    public static class TupleProtoSerializer extends ProtoSerializer<Tuple> {
        public TupleProtoSerializer() {
            super(Tuple.getDefaultInstance());
        }
    }

    private final T baseT;

    protected ProtoSerializer(final T baseT) {
        this.baseT = baseT;
    }

    @Override
    public ByteBuffer toByteBuffer(final T obj) {
        return ByteBuffer.wrap(obj.toByteArray());
    }

    @Override
    public T fromByteBuffer(final ByteBuffer byteBuffer) {
        try {
            return (T) baseT.newBuilderForType().mergeFrom(ByteString.copyFrom(byteBuffer)).build();
        } catch (final InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    }
}
