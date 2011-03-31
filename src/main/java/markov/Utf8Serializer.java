package markov;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

import com.google.common.base.Charsets;

public class Utf8Serializer extends AbstractSerializer<String> {
    private static final Utf8Serializer instance = new Utf8Serializer();

    public static Utf8Serializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(final String obj) {
        return ByteBuffer.wrap(obj.getBytes(Charsets.UTF_8));
    }

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
        return Charsets.UTF_8.decode(byteBuffer).toString();
    }
}