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

import com.twitter.algebird.Monoid;
import com.twitter.algebird.Monoid$;
import com.twitter.algebird.Semigroup;

/**
 *
 * help use the Monoids from Java
 *
 * @author Julien Le Dem
 *
 */
public class Monoids {

  private Monoids() {}

  private static final Monoid$ MODULE = Monoid$.MODULE$;

  private static final MethodRegistry registry = new MethodRegistry(MODULE, Monoid.class);

  /**
   * @param c the type of the elements
   * @return a monoid of c
   */
  public static <T> Monoid<T> monoid(Class<T> c) {
    @SuppressWarnings("unchecked")
    Monoid<T> m = (Monoid<T>)registry.resolveAndInvoke(c);
    return m;
  }

  public static Monoid<Boolean> boolMonoid() { return MODULE.jboolMonoid(); }
  public static Monoid<Integer> intMonoid() { return MODULE.jintMonoid(); }
  public static Monoid<Short> shortMonoid() { return MODULE.jshortMonoid(); }
  public static Monoid<scala.math.BigInt> bigIntMonoid() { return MODULE.bigIntMonoid(); }
  public static Monoid<Long> longMonoid() { return MODULE.jlongMonoid(); }
  public static Monoid<Float> floatMonoid() { return MODULE.jfloatMonoid(); }
  public static Monoid<Double> doubleMonoid() { return MODULE.jdoubleMonoid(); }
  public static Monoid<String> stringMonoid() { return MODULE.stringMonoid(); }
  public static <T> Monoid<scala.Option<T>> optionMonoid(Semigroup<T> componentSemigroup) { return MODULE.optionMonoid(componentSemigroup); }
  public static <T> Monoid<scala.collection.immutable.List<T>> listMonoid() { return MODULE.listMonoid(); }
  public static <T> Monoid<scala.collection.immutable.Seq<T>> seqMonoid() { return MODULE.seqMonoid(); }
  public static <T> Monoid<scala.collection.immutable.IndexedSeq<T>> indexedSeqMonoid(Monoid<T> componentMonoid) { return MODULE.indexedSeqMonoid(componentMonoid); }
  public static <T> Monoid<java.util.List<T>> jlistMonoid() { return MODULE.jlistMonoid(); }
  public static <T> Monoid<scala.collection.immutable.Set<T>> setMonoid() { return MODULE.setMonoid(); }
  public static <K, V> Monoid<scala.collection.immutable.Map<K,V>> mapMonoid(Semigroup<V> componentSemigroup) { return MODULE.mapMonoid(componentSemigroup); }
  public static <K, V> Monoid<scala.collection.Map<K,V>> scMapMonoid(Semigroup<V> componentSemigroup) { return MODULE.scMapMonoid(componentSemigroup); }
  public static <K, V> Monoid<java.util.Map<K,V>> jmapMonoid(Semigroup<V> componentSemigroup) { return MODULE.jmapMonoid(componentSemigroup); }
  public static <T> Monoid<scala.Function1<T,T>> function1Monoid() { return MODULE.function1Monoid(); }
}
