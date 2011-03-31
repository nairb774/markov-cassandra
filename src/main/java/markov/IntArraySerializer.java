package markov;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

public class IntArraySerializer extends AbstractSerializer<int[]> {
    private static final IntArraySerializer instance = new IntArraySerializer();

    public static IntArraySerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(final int[] obj) {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE * obj.length);
        buffer.asIntBuffer().put(obj);
        return buffer;
    }

    @Override
    public int[] fromByteBuffer(final ByteBuffer byteBuffer) {
        final int[] ints = new int[byteBuffer.remaining() / Integer.SIZE];
        byteBuffer.asIntBuffer().get(ints);
        byteBuffer.position(byteBuffer.position() + ints.length * Integer.SIZE);
        return ints;
    }
}