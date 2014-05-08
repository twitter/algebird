package com.twitter.algebird.javaapi;

import static com.twitter.algebird.javaapi.Monoids.monoid;
import static com.twitter.algebird.javaapi.Semigroups.semigroup;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestRegistry {
  @Test
  public void testSemigroups() {
    assertEquals(3L, semigroup(Long.class).plus(1L, 2L).longValue());
    assertEquals(3, semigroup(Integer.class).plus(1, 2).intValue());
    assertEquals("ab", semigroup(String.class).plus("a", "b"));
  }

  @Test
  public void testMonoids() {
    assertEquals(0L, monoid(Long.class).zero().longValue());
    assertEquals(0, monoid(Integer.class).zero().intValue());
    assertEquals("", monoid(String.class).zero());
  }
}
