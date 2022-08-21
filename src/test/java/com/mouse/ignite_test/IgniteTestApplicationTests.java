package com.mouse.ignite_test;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.ClientTransactions;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest
class IgniteTestApplicationTests {

    @Test
    void contextLoads() {
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        try (IgniteClient client = Ignition.startClient(cfg)) {
            //=========================Get data from the cache=========================//
            ClientCache<Integer, String> cache = client.createCache("myCache1");

            Map<Integer, String> data = new HashMap<>();
            for (int i = 1; i <= 100; i++) {
                Integer integer = i;
                if (data.put(integer, integer.toString()) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            }

            cache.putAll(data);

            assert !cache.replace(1, "2", "3");
            assert "1".equals(cache.get(1));
            assert cache.replace(1, "1", "3");
            assert "3".equals(cache.get(1));

            cache.put(101, "101");

            cache.removeAll(data.keySet());
            assert cache.size() == 1;
            assert "101".equals(cache.get(101));

            cache.removeAll();
            assert 0 == cache.size();


//            ClientCache<Integer, Person> personCache = client.getOrCreateCache("persons");
//
//            Query<Cache.Entry<Integer, Person>> qry = new ScanQuery<Integer, Person>(
//                    (i, p) -> p.getFirstName().contains("Joe"));
//
//            try (QueryCursor<Cache.Entry<Integer, Person>> cur = personCache.query(qry)) {
//                for (Cache.Entry<Integer, Person> entry : cur) {
//                    // Process the entry ...
//                    Integer key = entry.getKey();
//                    Person value = entry.getValue();
//                    System.out.println(key);
//                    System.out.println(value);
//                }
//            }
            //=============Working with Binary Objects================//
            IgniteBinary binary = client.binary();
            BinaryObject val = binary.builder("Person").setField("id", 1, int.class).setField("name", "Joe", String.class)
                    .build();

            ClientCache<Integer, BinaryObject> cachePerson = client.getOrCreateCache("persons").withKeepBinary();

            cachePerson.put(1, val);

            BinaryObject value = cachePerson.get(1);
            System.out.println(value);

            ClientTransactions tx = client.transactions();
            try (ClientTransaction t = tx.txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                ClientCache<Integer, String> transCache = client.getOrCreateCache("transCache");
                transCache.put(1, "new value");
                t.commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
