package markov;

import java.security.SecureRandom;
import java.util.Arrays;

public class RId {
    public static final RId minRId;
    public static final RId maxRId;

    static {
        minRId = new RId(new byte[16]);
        
        final byte[] bytes = new byte[16];
        Arrays.fill(bytes, (byte) 0xFF);
        maxRId = new RId(bytes);
    }

    private final byte[] bytes;

    public RId(final SecureRandom random) {
        bytes = new byte[16];
        random.nextBytes(bytes);
    }

    public RId(final byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RId other = (RId) obj;
        if (!Arrays.equals(bytes, other.bytes)) {
            return false;
        }
        return true;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
