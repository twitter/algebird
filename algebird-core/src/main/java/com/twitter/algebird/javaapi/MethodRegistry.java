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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * takes a <i>provider</i> object and looks for parameterless methods providing objects of type <i>provided</i>
 *
 * <i>provided</i> must take one type parameter.
 *
 * resolveAndInvoke(T.class) will return the corresponding <i>provided&lt;T&gt;</i> using the corresponding <i>provider</i> method
 *
 * @author Julien Le Dem
 *
 */
final class MethodRegistry {

  private final Map<Class<?>, Method> classToMethod = new HashMap<Class<?>, Method>();
  private final Object provider;
  private final Class<?> provided;

  /**
   * building a registry of provider functions for the <i>provided</i> class
   * @param provider object to look for methods
   * @param provided parameterized type taking exactly one parameter
   */
  MethodRegistry(Object provider, Class<?> provided) {
    this.provider = provider;
    this.provided = provided;
    TypeVariable<?>[] providedTypeParameters = provided.getTypeParameters();
    if (providedTypeParameters == null || providedTypeParameters.length != 1) {
      throw new IllegalArgumentException("provided must have exactly one type parameter. Found: " + Arrays.toString(providedTypeParameters));
    }
    Class<?> providerClass = provider.getClass();
    Set<Class<?>> conflictingDefinitions = new HashSet<Class<?>>();
    for (Method method : providerClass.getMethods()) {
      if (method.getParameterTypes().length == 0 && method.getReturnType().isAssignableFrom(provided)) {
        // method of signature {name}(): Semigroup[T]
        Type returnType = method.getGenericReturnType();
        if (returnType instanceof ParameterizedType) {
          Class<?> operandType;
          // get the Type argument from the return type Semigroup[T]
          Type type = ((ParameterizedType)returnType).getActualTypeArguments()[0];
          if (type instanceof ParameterizedType) {
            // if itself a parameterized type then just get the erased equivalent
            operandType = (Class<?>)((ParameterizedType)type).getRawType();
          } else {
            operandType = (Class<?>)type;
          }
          // because of type erasure scala primitive types just become Object
          if (operandType != Object.class
              && !conflictingDefinitions.contains(operandType)) {
            if (classToMethod.put(operandType, method) != null) {
              classToMethod.remove(operandType);
              // let's not create ambiguity
              conflictingDefinitions.add(operandType);
            }
          }
        }
      }
    }
  }

  private Method getMethod(Class<?> operandType) {
    Method method = classToMethod.get(operandType);
    if (method == null && operandType.getSuperclass() != null) {
      return getMethod(operandType.getSuperclass());
    } else {
      return method;
    }
  }

  private String typeName(Class<?> typeParameter) {
    return provided.getSimpleName() + "<" + typeParameter + ">";
  }

  /**
   * @param typeParameter
   * @return if found in <i>provider</i>, will return a <i>provided&lt;typeParameter&gt;</i>
   */
  Object resolveAndInvoke(Class<?> typeParameter) {
    Method method = getMethod(typeParameter);
    if (method != null) {
      try {
        return method.invoke(provider);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Could not resolve " + typeName(typeParameter) + " because " + method.getName() + "() threw an exception",  e.getTargetException());
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Should not happen. Could not resolve " + typeName(typeParameter) + " because " + method.getName() + "() is not accessible",  e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Should not happen. Could not call " + method.getName() + " on provider object",  e);
      }
    } else {
      throw new IllegalArgumentException(typeName(typeParameter) + " not found");
    }
  }
}
