package im.tad;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import javax.cache.Cache;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final String CACHE_NAME = "2022022516";
    private static final String TARGET_DATA = "2022022516";
    private static final String MESSAGE_GID = UUID.randomUUID().toString();
    public static void main(String[] args) {
        Logger logger = Logger.getAnonymousLogger();
        logger.addHandler(new ConsoleHandler());
        logger.setLevel(Level.INFO);

        IgniteConfiguration serverCfg = new IgniteConfiguration();
        serverCfg.setLifecycleBeans(new MyLifecycleBean());
        try (Ignite server = Ignition.start(serverCfg)) {
            IgniteCache<String, BloomFilter<String>> serverCache = server.getOrCreateCache(CACHE_NAME);
            logger.info("서버 캐시 이름 [" + serverCache.getName() + "]");

            IgniteSemaphore semaphore = server.semaphore("bloomFilterMaker", // Distributed semaphore name.
                    1, // Number of permits.
                    true, // Release acquired permits if node, that owned them, left topology.
                    true // Create if it doesn't exist.
            );

            ClientConfiguration clientCfg = new ClientConfiguration();
            clientCfg.setAddresses("127.0.0.1:10800");
            try(IgniteClient client = Ignition.startClient(clientCfg)){
                ClientCache<String, BloomFilter> cache = client.cache(CACHE_NAME);

                //실제 상황에서는 이미 만들어져 있을 수 있음
                BloomFilter bloomFilter = cache.get(MESSAGE_GID);
                if(bloomFilter == null) {
                    semaphore.acquire();
                    try {
                        bloomFilter = cache.get(MESSAGE_GID); //다른 곳에서 만들어 졌을 수 있음. 한번 더 확인
                        if(bloomFilter == null) {
                            bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 1000, 0.01);
                            cache.put(MESSAGE_GID, bloomFilter);
                        }
                    }finally {
                        semaphore.release();
                    }
                }

                bloomFilter.put(TARGET_DATA);

                ScanQuery<String, BloomFilter<String>> scanQuery = new ScanQuery<>((key, filter) -> filter.mightContain(TARGET_DATA));
                try(QueryCursor<Cache.Entry<String, BloomFilter<String>>> qryCursor = cache.query(scanQuery)){
                    qryCursor.forEach(entry -> logger.info("데이터 포함 필터 이름 [" + entry.getKey() + "]"));
                }
            }
        }
    }
}