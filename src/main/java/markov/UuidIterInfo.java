package markov;

import java.util.UUID;

public class UuidIterInfo implements CF.IterInfo<UUID> {
    @Override
    public UUID next(final UUID name) {
        long least = name.getLeastSignificantBits();
        long most = name.getMostSignificantBits();
        if (least == Long.MAX_VALUE) {
            if (most == Long.MAX_VALUE) {
                return null;
            }
            least = Long.MIN_VALUE;
            most += 1;
        } else {
            least += 1;
        }
        return new UUID(most, least);
    }
}
