package markov;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import com.google.common.collect.Maps;
import com.google.inject.TypeLiteral;

public class CF<K> {
    public interface IterInfo<N> {
        N next(final N name);
    }

    private class CFIterable<N, V> implements Iterable<HColumn<N, V>> {
        private final K key;
        private final Serializer<N> nameSerializer;
        private final IterInfo<N> iterInfo;
        private final Serializer<V> valueSerializer;

        private CFIterable(final K key, final IterInfo<N> iterInfo, Class<V> valueType) {
            this.key = key;
            this.iterInfo = iterInfo;
            final ParameterizedType type = (ParameterizedType) TypeLiteral.get(iterInfo.getClass())
                    .getSupertype(IterInfo.class).getType();
            final Type baseType = type.getActualTypeArguments()[0];
            if (!(baseType instanceof Class)) {
                throw new IllegalStateException(baseType.toString());
            }
            this.nameSerializer = typeMap.getSerializer((Class<N>) baseType);
            this.valueSerializer = typeMap.getSerializer(valueType);
        }

        @Override
        public Iterator<HColumn<N, V>> iterator() {
            return new CFIterator<N, V>(key, iterInfo, nameSerializer, valueSerializer);
        }
    }

    private class CFIterator<N, V> implements Iterator<HColumn<N, V>> {
        private final int blockSize = 64;
        private Iterator<HColumn<N, V>> columns;
        private boolean finished = false;
        private final K key;
        private HColumn<N, V> lastColumn;
        private N minName;
        private final Serializer<N> nameSerializer;
        private final IterInfo<N> iterInfo;
        private final Serializer<V> valueSerializer;

        private CFIterator(K key, final IterInfo<N> iterInfo, Serializer<N> nameSerializer,
                Serializer<V> valueSerializer) {
            this.key = key;
            this.minName = null;
            this.iterInfo = iterInfo;
            this.nameSerializer = nameSerializer;
            this.valueSerializer = valueSerializer;
            loadNext();
        }

        @Override
        public boolean hasNext() {
            if (!columns.hasNext()) {
                if (finished) {
                    return false;
                }
                minName = iterInfo.next(lastColumn.getName());
                if (minName == null) { // There is no next name
                    finished = true;
                } else {
                    loadNext();
                }
                return hasNext();
            }
            return true;
        }

        private void loadNext() {
            final List<HColumn<N, V>> list = HFactory
                    .createSliceQuery(keyspace, serializer, nameSerializer, valueSerializer).setKey(key)
                    .setColumnFamily(cfName).setRange(minName, null, false, blockSize).execute().get().getColumns();
            if (list.size() < blockSize) {
                finished = true;
            }
            columns = list.iterator();
        }

        @Override
        public HColumn<N, V> next() {
            return lastColumn = columns.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public class CFKey {
        private final K key;

        private CFKey(final K key) {
            this.key = key;
        }

        public <N, V> CFKey add(final N name, final V value) {
            CF.this.add(key, name, value);
            return this;
        }

        public <N, V> Iterable<HColumn<N, V>> iter(final IterInfo<N> iterInfo, final Class<V> valueType) {
            return new CFIterable<N, V>(key, iterInfo, valueType);
        }
    }

    public static class TypeMap {
        private final Map<Class<?>, Serializer<?>> typeMap = Maps.newHashMap();

        @Inject
        public TypeMap(final Set<Serializer<?>> serializers) {
            for (final Serializer<?> s : serializers) {
                register(s);
            }
        }

        private <T> Serializer<T> getSerializer(final Class<T> t) {
            final Serializer<T> serializer = (Serializer<T>) typeMap.get(t);
            if (serializer == null) {
                throw new IllegalStateException("Serializer for class " + t.getName() + " not configured.");
            }
            return serializer;
        }

        private <T> Serializer<T> getSerializer(final T t) {
            return getSerializer((Class<T>) t.getClass());
        }

        public <T> TypeMap register(final Serializer<T> serializer) {
            final ParameterizedType type = (ParameterizedType) TypeLiteral.get(serializer.getClass())
                    .getSupertype(Serializer.class).getType();
            Type baseType = type.getActualTypeArguments()[0];
            if (baseType instanceof GenericArrayType) {
                baseType = getArrayClass((GenericArrayType) baseType);
            }
            if (!(baseType instanceof Class)) {
                throw new IllegalStateException(baseType.getClass().getCanonicalName());
            }
            typeMap.put((Class<?>) baseType, serializer);
            return this;
        }

        private Class<?> getArrayClass(final GenericArrayType type) {
            final Type genericComponentType = type.getGenericComponentType();
            final Class<?> baseType;
            if (genericComponentType instanceof GenericArrayType) {
                try {
                    baseType = getArrayClass((GenericArrayType) genericComponentType);
                } catch (final IllegalStateException e) {
                    throw new IllegalStateException(type.toString(), e);
                }
            } else if (genericComponentType instanceof Class) {
                baseType = (Class<?>) genericComponentType;
            } else {
                throw new IllegalStateException(type.toString(), new IllegalStateException(genericComponentType
                        .getClass().getCanonicalName()));
            }
            return Array.newInstance(baseType, 0).getClass();
        }
    }

    private final String cfName;
    private int count = 0;
    private final Keyspace keyspace;
    private final int maxCount = 16;
    private Mutator<K> mutator;
    private final Object mutatorLock = new Object();
    private final Serializer<K> serializer;
    private final TypeMap typeMap;

    public CF(final Keyspace keyspace, final Class<K> keyType, final TypeMap typeMap) {
        this.keyspace = keyspace;
        this.cfName = keyType.getSimpleName();
        this.serializer = typeMap.getSerializer(keyType);
        this.typeMap = typeMap;
        mutator = HFactory.createMutator(keyspace, serializer);
    }

    private <N, V> void add(final K key, final N name, final V value) {
        final Serializer<N> nameSer = typeMap.getSerializer(name);
        final Serializer<V> valueSer = typeMap.getSerializer(value);
        final HColumn<N, V> column = HFactory.createColumn(name, value, nameSer, valueSer);

        Mutator<K> oldMutator = null;
        synchronized (mutatorLock) {
            mutator.addInsertion(key, cfName, column);
            count += 1;
            if (count == maxCount) {
                count = 0;
                oldMutator = mutator;
                mutator = HFactory.createMutator(keyspace, serializer);
            }
        }
        if (oldMutator != null) {
            oldMutator.execute();
        }
    }

    public CF<K> flush() {
        Mutator<K> oldMutator = null;
        synchronized (mutatorLock) {
            count = 0;
            oldMutator = mutator;
            mutator = HFactory.createMutator(keyspace, serializer);
        }
        oldMutator.execute();
        return this;
    }

    public CFKey forKey(final K key) {
        return new CFKey(key);
    }
}
