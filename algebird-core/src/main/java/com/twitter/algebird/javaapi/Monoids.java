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

/**
 *
 * help use the Monoids from Java
 *
 * @author Julien Le Dem
 *
 */
public class Monoids {

  private Monoids() {}

  private static MethodRegistry registry = new MethodRegistry(Monoid$.MODULE$, Monoid.class);

  /**
   * @param c the type of the elements
   * @return a monoid of c
   */
  public static <T> Monoid<T> monoid(Class<T> c) {
    @SuppressWarnings("unchecked")
    Monoid<T> m = (Monoid<T>)registry.resolveAndInvoke(c);
    return m;
  }
}
