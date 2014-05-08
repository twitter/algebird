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

  private static MethodRegistry registry = new MethodRegistry(Semigroup$.MODULE$, Semigroup.class);

  /**
   * @param c the type of the elements
   * @return a semigroup of c
   */
  public static <T> Semigroup<T> semigroup(Class<T> c) {
    @SuppressWarnings("unchecked")
    Semigroup<T> sg = (Semigroup<T>)registry.resolveAndInvoke(c);
    return sg;
  }
}
