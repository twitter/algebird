/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.algebird.javaapi;

import com.twitter.algebird.Semigroup;
import com.twitter.algebird.Semigroup$;

/**
*
* helps use the Semigroups from Java
*
* @author Julien Le Dem
*
*/
public class Semigroups {

  private Semigroups() {}

  private static final Semigroup$ MODULE = Semigroup$.MODULE$;
  private static final MethodRegistry registry = new MethodRegistry(MODULE, Semigroup.class);

  /**
   * @param c the type of the elements
   * @return a semigroup of c
   */
  public static <T> Semigroup<T> semigroup(Class<T> c) {
    @SuppressWarnings("unchecked")
    Semigroup<T> sg = (Semigroup<T>)registry.resolveAndInvoke(c);
    return sg;
  }

  public static Semigroup<Boolean> boolSemigroup() { return MODULE.jboolSemigroup(); }
  public static Semigroup<Integer> intSemigroup() { return MODULE.jintSemigroup(); }
  public static Semigroup<Short> shortSemigroup() { return MODULE.jshortSemigroup(); }
  public static Semigroup<scala.math.BigInt> bigIntSemigroup() { return MODULE.bigIntSemigroup(); }
  public static Semigroup<Long> longSemigroup() { return MODULE.jlongSemigroup(); }
  public static Semigroup<Float> floatSemigroup() { return MODULE.jfloatSemigroup(); }
  public static Semigroup<Double> doubleSemigroup() { return MODULE.jdoubleSemigroup(); }
  public static Semigroup<String> stringSemigroup() { return MODULE.stringSemigroup(); }
  public static <T> Semigroup<scala.Option<T>> optionSemigroup(Semigroup<T> componentSemigroup) { return MODULE.optionSemigroup(componentSemigroup); }
  public static <T> Semigroup<scala.collection.immutable.List<T>> listSemigroup() { return MODULE.listSemigroup(); }
  public static <T> Semigroup<scala.collection.Seq<T>> seqSemigroup() { return MODULE.seqSemigroup(); }
  public static <T> Semigroup<scala.collection.IndexedSeq<T>> indexedSeqSemigroup(Semigroup<T> componentSemigroup) { return MODULE.indexedSeqSemigroup(componentSemigroup); }
  public static <T> Semigroup<java.util.List<T>> jlistSemigroup() { return MODULE.jlistSemigroup(); }
  public static <T> Semigroup<scala.collection.immutable.Set<T>> setSemigroup() { return MODULE.setSemigroup(); }
  public static <K, V> Semigroup<scala.collection.immutable.Map<K,V>> mapSemigroup(Semigroup<V> componentSemigroup) { return MODULE.mapSemigroup(componentSemigroup); }
  public static <K, V> Semigroup<scala.collection.Map<K,V>> scMapSemigroup(Semigroup<V> componentSemigroup) { return MODULE.scMapSemigroup(componentSemigroup); }
  public static <K, V> Semigroup<java.util.Map<K,V>> jmapSemigroup(Semigroup<V> componentSemigroup) { return MODULE.jmapSemigroup(componentSemigroup); }
  public static <T> Semigroup<scala.Function1<T,T>> function1Semigroup() { return MODULE.function1Semigroup(); }
}
