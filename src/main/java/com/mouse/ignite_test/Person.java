package com.mouse.ignite_test;

import lombok.Data;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/8/21 20:34
 */
@Data
public class Person implements Serializable {
    /** Indexed field. Will be visible for SQL engine. */
    @QuerySqlField(index = true)
    private long id;

    /** Queryable field. Will be visible for SQL engine. */
    @QuerySqlField(index = true)
    private String name;

    /** Will NOT be visible for SQL engine. */
//    private int age;

    /** Indexed field. Will be visible for SQL engine. */
//    @QuerySqlField(index = true)
//    private Address address;
}
