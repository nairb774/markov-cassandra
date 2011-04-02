package markov;

import javax.inject.Inject;

import markov.MarkovProto.Source;
import me.prettyprint.hector.api.Cluster;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class Main {

    /**
     * @param args
     */
    public static void main(String[] args) {
        final Injector injector = Guice.createInjector(new MarkovModule());
        final Cluster cluster = injector.getInstance(Cluster.class);
        try {
            injector.getInstance(Main.class).run();
        } finally {
            cluster.getConnectionManager().shutdown();
        }
    }

    private final MarkovSchema markovSchema;
    private final Markov markov;

    @Inject
    public Main(final MarkovSchema markovSchema, final Markov markov) {
        this.markovSchema = markovSchema;
        this.markov = markov;
    }

    public void run() {
        markovSchema.setup();
        markov.addString(Source.newBuilder().setName("irc").build(),
                "this is just a test of the internal logic. this is just a test fo the internal logic");
    }
}
