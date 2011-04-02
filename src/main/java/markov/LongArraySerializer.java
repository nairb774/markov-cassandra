package markov;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

public class LongArraySerializer extends AbstractSerializer<long[]> {
    private static final LongArraySerializer instance = new LongArraySerializer();

    public static LongArraySerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(final long[] obj) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE * obj.length);
        buffer.asLongBuffer().put(obj);
        return buffer;
    }

    @Override
    public long[] fromByteBuffer(final ByteBuffer byteBuffer) {
        final long[] longs = new long[byteBuffer.remaining() / Long.SIZE];
        byteBuffer.asLongBuffer().get(longs);
        byteBuffer.position(byteBuffer.position() + longs.length * Long.SIZE);
        return longs;
    }
}
