package bifrore.common.store;

import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStore;
import io.micrometer.core.instrument.Metrics;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class PersistentMapStore<K, V> implements MapStore<K, V>, MapLoader<K, V> {
    private final String dataDirPrefix = System.getProperty("DATA_DIR", ".");
    private final RocksDB rocksDB;
    private final Function<K, byte[]> keySerializer;
    private final Function<byte[], K> keyDeserializer;
    private final Function<V, byte[]> valueSerializer;
    private final Function<byte[], V> valueDeserializer;

    public PersistentMapStore(String dbPath,
                              String storeName,
                              Function<K, byte[]> keySerializer,
                              Function<byte[], K> keyDeserializer,
                              Function<V, byte[]> valueSerializer,
                              Function<byte[], V> valueDeserializer) throws RocksDBException {
        RocksDB.loadLibrary();
        long cacheCapacity = 16 * 1024 * 1024;
        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockCache(new LRUCache(cacheCapacity));
        Statistics stat = new Statistics();
        Options options = new Options()
                .setCreateIfMissing(true)
                .setAllowMmapReads(true)
                .setTableFormatConfig(tableOptions)
                .setStatistics(stat);
        Metrics.gauge(storeName + "." + "rocksdb.block_cache_hits",
                stat.getTickerCount(TickerType.BLOCK_CACHE_HIT));
        Metrics.gauge(storeName + "." + "rocksdb.block_cache_misses",
                stat.getTickerCount(TickerType.BLOCK_CACHE_MISS));
        this.rocksDB = RocksDB.open(options, this.dataDirPrefix + "/" + dbPath);
        this.keySerializer = keySerializer;
        this.keyDeserializer = keyDeserializer;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void store(K k, V v) {
        try {
            rocksDB.put(keySerializer.apply(k), valueSerializer.apply(v));
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to store data in RocksDB", e);
        }
    }

    @Override
    public void storeAll(Map<K, V> map) {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = new WriteOptions()) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                batch.put(keySerializer.apply(entry.getKey()), valueSerializer.apply(entry.getValue()));
            }
            rocksDB.write(options, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to store multiple entries in RocksDB", e);
        }
    }

    @Override
    public void delete(K k) {
        try {
            rocksDB.delete(keySerializer.apply(k));
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to delete data from RocksDB", e);
        }
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        for (K key : keys) {
            delete(key);
        }
    }

    @Override
    public V load(K k) {
        try {
            byte[] valueBytes = rocksDB.get(keySerializer.apply(k));
            return valueDeserializer.apply(valueBytes);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to load data from RocksDB", e);
        }
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        Map<K, V> map = new HashMap<>();
        for (K key : keys) {
            V value = load(key);
            if (value != null) {
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        List<K> keys = new ArrayList<>();
        try (RocksIterator iterator = rocksDB.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] keyBytes = iterator.key();
                K key = keyDeserializer.apply(keyBytes);
                keys.add(key);
                iterator.next();
            }
        }
        return keys;
    }
}
