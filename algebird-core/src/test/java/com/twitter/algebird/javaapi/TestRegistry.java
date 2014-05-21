package com.twitter.algebird.javaapi;

import static com.twitter.algebird.javaapi.Monoids.*;
import static com.twitter.algebird.javaapi.Semigroups.*;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static scala.math.BigInt.int2bigInt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import scala.math.BigInt;

public class TestRegistry {

  @Test
  public void testSemigroups() {

    assertEquals(3L, semigroup(Long.class).plus(1L, 2L).longValue());
    assertEquals(3L, longSemigroup().plus(1L, 2L).longValue());

    assertEquals(3, semigroup(Integer.class).plus(1, 2).intValue());
    assertEquals(3, intSemigroup().plus(1, 2).intValue());

    assertEquals(3f, semigroup(Float.class).plus(1f, 2f).floatValue(), 0.01);
    assertEquals(3f, floatSemigroup().plus(1f, 2f).floatValue(), 0.01);

    assertEquals(3d, semigroup(Double.class).plus(1d, 2d).doubleValue(), 0.01);
    assertEquals(3d, doubleSemigroup().plus(1d, 2d).doubleValue(), 0.01);

    assertEquals(3, semigroup(Integer.class).plus(1, 2).intValue());
    assertEquals(3, intSemigroup().plus(1, 2).intValue());

    assertEquals(false, semigroup(Boolean.class).plus(true, true).booleanValue());
    assertEquals(false, boolSemigroup().plus(true, true).booleanValue());

    assertEquals(int2bigInt(3), semigroup(BigInt.class).plus(int2bigInt(1), int2bigInt(3)));
    assertEquals(int2bigInt(3), bigIntSemigroup().plus(int2bigInt(1), int2bigInt(3)));

    assertEquals("ab", semigroup(String.class).plus("a", "b"));
    assertEquals("ab", stringSemigroup().plus("a", "b"));

    Map<String, Long> map1 = new HashMap<String, Long>();
    map1.put("foo", 1L);
    map1.put("bar", 2L);
    Map<String, Long> map2 = new HashMap<String, Long>();
    map2.put("foo", 3L);
    map2.put("bar", 4L);
    Map<String, Long> map3 = new HashMap<String, Long>();
    map3.put("foo", 4L);
    map3.put("bar", 6L);
    assertEquals(map3, Semigroups.<String, Long>jmapSemigroup(longSemigroup()).plus(map1, map2));

    assertEquals(EMPTY_LIST, semigroup(List.class).plus(EMPTY_LIST, EMPTY_LIST));
    assertEquals(emptyList(), jlistSemigroup().plus(emptyList(), emptyList()));

    }

  @Test
  public void testMonoids() {
    assertEquals(0L, monoid(Long.class).zero().longValue());
    assertEquals(0L, longMonoid().zero().longValue());
    assertEquals(0, monoid(Integer.class).zero().intValue());
    assertEquals(0, intMonoid().zero().intValue());
    assertEquals("", monoid(String.class).zero());
    assertEquals("", stringMonoid().zero());

    Map<String, Long> map1 = new HashMap<String, Long>();
    map1.put("foo", 1L);
    map1.put("bar", 2L);
    Map<String, Long> map2 = new HashMap<String, Long>();
    map2.put("foo", 3L);
    map2.put("bar", 4L);
    Map<String, Long> map3 = new HashMap<String, Long>();
    map3.put("foo", 4L);
    map3.put("bar", 6L);
    assertEquals(map3, Monoids.<String, Long>jmapMonoid(longSemigroup()).plus(map1, map2));

    assertEquals(EMPTY_LIST, monoid(List.class).plus(EMPTY_LIST, EMPTY_LIST));
    assertEquals(emptyList(), jlistMonoid().plus(emptyList(), emptyList()));
  }
}
