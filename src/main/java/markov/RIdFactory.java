package markov;

import java.security.SecureRandom;

public class RIdFactory {
    private static final RIdFactory instance = new RIdFactory();

    public static RIdFactory get() {
        return instance;
    }

    private final SecureRandom random;

    private RIdFactory() {
        this(new RId(new SecureRandom()));
    }

    public RIdFactory(final RId rId) {
        random = new SecureRandom(rId.getBytes());
    }

    public RId next() {
        return new RId(random);
    }
}
