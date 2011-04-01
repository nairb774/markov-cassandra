package markov;

import java.util.List;

import javax.inject.Inject;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import com.google.common.collect.Lists;

public class MarkovSchema {
    private final Cluster cluster;
    
    @Inject
    public MarkovSchema(final Cluster cluster) {
        this.cluster = cluster;
    }
    
    public void setup() {
        final KeyspaceDefinition describeKeyspace = cluster.describeKeyspace("main");
        if (describeKeyspace == null) {
            final List<ColumnFamilyDefinition> mainColumns = Lists.newArrayList();
            mainColumns.add(defineChain());
            mainColumns.add(defineTuple());
            cluster.addKeyspace(HFactory.createKeyspaceDefinition("main",
                    "org.apache.cassandra.locator.SimpleStrategy", 3, mainColumns));
            return;
        }
        ensureColumnFamily(describeKeyspace, defineChain());
        ensureColumnFamily(describeKeyspace, defineTuple());
    }
    
    private void ensureColumnFamily(final KeyspaceDefinition describeKeyspace, final ColumnFamilyDefinition cf) {
        boolean found = false;
        for (final ColumnFamilyDefinition columnFamilyDefinition : describeKeyspace.getCfDefs()) {
            if (cf.getName().equals(columnFamilyDefinition.getName())) {
                found = true;
                break;
            }
        }
        if (!found) {
            cluster.addColumnFamily(cf);
        }
    }
    
    private ColumnFamilyDefinition defineChain() {
        return HFactory.createColumnFamilyDefinition("main", "Chain", ComparatorType.LEXICALUUIDTYPE);
    }
    
    private ColumnFamilyDefinition defineTuple() {
        return HFactory.createColumnFamilyDefinition("main", "Tuple", ComparatorType.UTF8TYPE);
    }
}
