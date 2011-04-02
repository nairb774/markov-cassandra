package markov;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

public class RIdSerializer extends AbstractSerializer<RId> {
    @Override
    public ByteBuffer toByteBuffer(final RId obj) {
        return ByteBuffer.wrap(obj.getBytes());
    }

    @Override
    public RId fromByteBuffer(final ByteBuffer byteBuffer) {
        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new RId(bytes);
    }
}
