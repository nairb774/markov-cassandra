package markov;


public class RIdIterInfo implements CF.IterInfo<RId> {
    @Override
    public RId next(final RId name) {
        final byte[] bytes = name.getBytes().clone();
        int i = bytes.length - 1;
        while (i >= 0) {
            final int b = bytes[i] & 0xFF;
            if (b != 0xff) {
                bytes[i] = (byte) (b + 1);
                break;
            }
            bytes[i] = 0;
            i -= 1;
        }
        if (i == -1) {
            return null;
        }
        return new RId(bytes);
    }
}
