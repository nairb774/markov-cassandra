package markov;

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
            final Markov markov = injector.getInstance(Markov.class);
            markov.addString("this is just a test of the internal logic. this is just a test fo the internal logic");
        } finally {
            cluster.getConnectionManager().shutdown();
        }
    }

}
