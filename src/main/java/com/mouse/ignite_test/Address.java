package com.mouse.ignite_test;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/8/21 20:34
 */
public class Address {
    /** Indexed field. Will be visible for SQL engine. */
    @QuerySqlField(index = true)
    private String street;

    /** Indexed field. Will be visible for SQL engine. */
    @QuerySqlField(index = true)
    private int zip;
}