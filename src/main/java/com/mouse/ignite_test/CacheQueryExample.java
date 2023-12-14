/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mouse.ignite_test;

import javax.cache.Cache;

import com.mouse.ignite_test.model.Organization;
import com.mouse.ignite_test.model.Person;
import lombok.val;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.springframework.util.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;

/**
 * Cache queries example. This example demonstrates TEXT, FULL SCAN and INDEX
 * queries over cache.
 * <p>
 * Example also demonstrates usage of fields queries that return only required
 * fields instead of whole key-value pairs. When fields queries are distributed
 * across several nodes, they may not work as expected. Keep in mind following
 * limitations (not applied if data is queried from one node only):
 * <ul>
 *     <li>
 *         Non-distributed joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to {@link AffinityKey} javadoc for more details.
 *         <p>
 *         To use distributed joins it is necessary to set query 'distributedJoin' flag using
 *         {@link SqlFieldsQuery#setDistributedJoins(boolean)}.
 *     </li>
 *     <li>
 *         Note that if you created query on to replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you created it on partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <p>
 * Remote nodes should be started using {@link ExampleNodeStartup} which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheQueryExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    /** Persons collocated with Organizations cache name. */
    private static final String PERSON_CACHE = CacheQueryExample.class.getSimpleName() + "Persons";

    static List<Person> personList = new ArrayList<>();
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);

            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<AffinityKey<Long>, Person> personCacheCfg =
                new CacheConfiguration<>(PERSON_CACHE);

            personCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            personCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);
            try {
                // Create caches.
//                ignite.getOrCreateCache(orgCacheCfg);
                ignite.getOrCreateCache(personCacheCfg);

                // Populate caches.
                initialize();

                // Example for SCAN-based query based on a predicate.
//                scanQuery();

                // Example for TEXT-based querying for a given string in peoples resumes.
//                textQuery();

                // Example for INDEX-based query with index criteria.
                indexQuery();
            }
            finally {
                // Distributed cache could be removed from cluster only by Ignite.destroyCache() call.
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            print("Cache query example finished.");
        }
    }

    /**
     * Example for scan query based on a predicate using binary objects.
     */
    private static void scanQuery() {
        IgniteCache<BinaryObject, BinaryObject> cache = Ignition.ignite()
            .cache(PERSON_CACHE).withKeepBinary();

        ScanQuery<BinaryObject, BinaryObject> scan = new ScanQuery<>(
            new IgniteBiPredicate<BinaryObject, BinaryObject>() {
                @Override public boolean apply(BinaryObject key, BinaryObject person) {
                    return person.<Double>field("salary") <= 1000;
                }
            }
        );

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000 (queried with SCAN query): ", cache.query(scan).getAll());
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     */
    private static void textQuery() {
        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        //  Query for all people with "Master Degree" in their resumes.
        QueryCursor<Cache.Entry<Long, Person>> masters =
            cache.query(new TextQuery<Long, Person>(Person.class, "Master"));

        // Query for all people with "Bachelor Degree" in their resumes.
        QueryCursor<Cache.Entry<Long, Person>> bachelors =
            cache.query(new TextQuery<Long, Person>(Person.class, "Bachelor"));

        print("Following people have 'Master Degree' in their resumes: ", masters.getAll());
        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.getAll());
    }

    /**
     * Example for query indexes with criteria and binary objects.
     */
    private static void indexQuery() {
        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE);

        // Query for all people who work in the organization "ApacheIgnite".
        val sw = new StopWatch();


        sw.start("in memory 1");
        personList.stream().filter(p -> p.orgId.equals(1L)).collect(Collectors.toList());
        sw.stop();

        sw.start("ignite 1.1");
        //不知道为什么这里好像没有使用索引，导致效果不如 cache.withKeepBinary()
        QueryCursor<Cache.Entry<Long, Person>> igniters = cache.query(
            new IndexQuery<Long, Person>(Person.class)
                .setCriteria(eq("orgId", 1L))
        );
        sw.stop();

        sw.start("ignite 1.2");
        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> org = cache.withKeepBinary().query(
                new IndexQuery<BinaryObject, BinaryObject>(Person.class.getName())
                        .setCriteria(gt("orgId", 1L)));
        sw.stop();
        print("Following people work in the 'ApacheIgnite' organization (queried with INDEX query): ",
            igniters.getAll());

        sw.start("in memory 2");
        personList.stream().filter(p -> p.orgId.equals(2L) && p.salary > 1500.0).collect(Collectors.toList());
        sw.stop();

        sw.start("ignite 2.1");
        // Query for all people who work in the organization "Other" and have salary more than 1,500.
        QueryCursor<Cache.Entry<Long, Person>> others = cache.query(
            new IndexQuery<Long, Person>(Person.class)  // Index name {@link Person#ORG_SALARY_IDX} is optional.
                .setCriteria(eq("orgId", 2L), gt("salary", 1500.0)));
        sw.stop();
        print("Following people work in the 'Other' organizations and have salary more than 1500 (queried with INDEX query): ",
            others.getAll());

        sw.start("in memory 3");
        personList.stream().filter(p -> p.salary > 1500.0).collect(Collectors.toList());
        sw.stop();

        sw.start("ignite 3.1");
        // Query for all people who have salary more than 1,500 using BinaryObject.
        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> rich = cache.withKeepBinary().query(
            new IndexQuery<BinaryObject, BinaryObject>(Person.class.getName())
                .setCriteria(gt("salary", 1500.0)));
        sw.stop();
        print("Following people have salary more than 1500 (queried with INDEX query and using binary objects): ",
            rich.getAll());

        sw.start("in memory 4");
        personList.stream().filter(p -> p.salary > 1500.0 && p.resume.contains("Master")).collect(Collectors.toList());
        for (val p:personList) {

        }
        sw.stop();

        sw.start("ignite 4.1");
        // Query for all people who have salary more than 1,500 and have 'Master Degree' in their resumes.
        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> richMasters = cache.withKeepBinary().query(
            new IndexQuery<BinaryObject, BinaryObject>(Person.class.getName())
                .setCriteria(gt("salary", 1500.0))
                .setFilter((k, v) -> v.<String>field("resume").contains("Master"))
        );
        sw.stop();
        sw.start("ignite 4.2 loop 只能迭代一次 游标已经置位");
        for (val p:richMasters) {

        }
        sw.stop();

//        print("Following people have salary more than 1500 and Master degree (queried with INDEX query): ",
//            richMasters.getAll());

        sw.start("in memory 5");
        personList.stream().filter(p -> p.resume.contains("Master")).collect(Collectors.toList());
        sw.stop();

        sw.start("ignite 5.1 string index");
        // Query for all people who have salary more than 1,500 and have 'Master Degree' in their resumes.
        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> masters = cache.withKeepBinary().query(
                new IndexQuery<BinaryObject, BinaryObject>(Person.class.getName())
                        .setFilter((k, v) -> v.<String>field("resume").contains("Master")));
        sw.stop();

        sw.start("in memory 6 no string index");
        val list6 = personList.stream().filter(p -> p.lastName.contains("10")).collect(Collectors.toList());
        sw.stop();

        sw.start("ignite 6.1 no string index");
        val list6ignite = new ArrayList<>();
        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> lastname10 = cache.withKeepBinary().query(
                new IndexQuery<BinaryObject, BinaryObject>(Person.class.getName())
                        .setFilter((k, v) -> v.<String>field("lastname").contains("10")));
        for (val p10: lastname10) {
            list6ignite.add(p10);
        }
        sw.stop();

        sw.start("in memory 7 no string index");
        val list7 = personList.stream().filter(p -> p.firstName.equals("John30000")).collect(Collectors.toList());
        sw.stop();

        sw.start("ignite 7 no string index");
        val list7ignite = new ArrayList<>();
        QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> firstname10 = cache.withKeepBinary().query(
                new IndexQuery<BinaryObject, BinaryObject>(Person.class.getName())
                        .setFilter((k, v) -> v.<String>field("firstName").equals("John30000")));
        for (val p10: firstname10) {
            list7ignite.add(p10);
        }
        sw.stop();

        sw.start("ignite 9  string index");
        SqlFieldsQuery sql9 = new SqlFieldsQuery(
                "select * from Persons");

// Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(sql9)) {
            for (List<?> row : cursor)
                System.out.println("personName=" + row.get(0));
        }
        sw.stop();

        System.out.println(sw.prettyPrint());

    }

    private static void initIgnite() {
        IgniteConfiguration cfg = new IgniteConfiguration();

// 创建数据区域配置
        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();
        dataRegionCfg.setMaxSize(4L * 1024); // 设置数据区域的最大大小为4KB

// 将数据区域配置添加到数据存储配置中
        dataStorageCfg.setDataRegionConfigurations(dataRegionCfg);

// 将数据存储配置添加到Ignite配置中
        cfg.setDataStorageConfiguration(dataStorageCfg);

// 启动Ignite节点
        Ignition.start(cfg);

    }
    /**
     * Populate cache with test data.
     */
    private static void initialize() {
//        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE);
//
//        // Clear cache before running the example.
//        orgCache.clear();
//
//        // Organizations.
//        Organization org1 = new Organization("ApacheIgnite");
//        Organization org2 = new Organization("Other");
//
//        val orgList = new ArrayList<Organization>();
//        for (int i = 0;i<20; i++) {
//            Organization oi = new Organization("i");
//            orgList.add(oi);
//        }
//
//        orgCache.put(org1.id(), org1);
//        orgCache.put(org2.id(), org2);

        IgniteCache<AffinityKey<Long>, Person> colPersonCache = Ignition.ignite().cache(PERSON_CACHE);
        // Clear caches before running the example.
        colPersonCache.clear();

        // People. 10000
        val r = new Random();
        for (int i = 0;i< 100000;i ++) {
            Person p = new Person(new Organization(), "John" + i, "Doe" + i, r.nextInt(10000), "John Doe has Master Degree." + i);
            colPersonCache.put(p.key(), p);
            personList.add(p);
            System.out.println(" person add i=" + i);
        }
//        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
//        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
//        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
//        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
//        colPersonCache.put(p1.key(), p1);
//        colPersonCache.put(p3.key(), p3);
//        colPersonCache.put(p4.key(), p4);
    }

    /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        print(msg);
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next);
    }
}
