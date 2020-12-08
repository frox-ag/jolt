/*
 * Copyright 2013 Bazaarvoice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bazaarvoice.jolt;

import com.bazaarvoice.jolt.common.Optional;
import com.bazaarvoice.jolt.common.tree.WalkedPath;
import com.bazaarvoice.jolt.exception.SpecException;
import com.bazaarvoice.jolt.order.ArraySortCompositeSpec;
import java.util.Map;
import javax.inject.Inject;

/** The ArraySortTransform sort of input JSON data elements inside array
 * based on criteria from spec. 
 */
public class ArraySortTransform implements SpecDriven, Transform {

  protected static final String ROOT_KEY = "root";
  private final ArraySortCompositeSpec rootSpec;

  /**
   * Initialize a ArraySort transform with a ArraySortCompositeSpec.
   *
   * @throws SpecException for a malformed spec
   */
  @Inject
  public ArraySortTransform(Object spec) {

    if (spec == null) {
      throw new SpecException("ArraySortTransform expected a spec of Map type, got 'null'.");
    }
    if (!(spec instanceof Map)) {
      throw new SpecException(
          "ArraySortTransform expected a spec of Map type, got " + spec.getClass().getSimpleName());
    }

    rootSpec = new ArraySortCompositeSpec(ROOT_KEY, (Map<String, Object>) spec);
  }

  /**
   * Applies the ArraySort transform.
   *
   * @param input the JSON object to transform
   * @return the output object with data shifted to it
   * @throws com.bazaarvoice.jolt.exception.TransformException for a malformed spec or if there are
   *     issues during the transform
   */
  @Override
  public Object transform(Object input) {

    rootSpec.apply(ROOT_KEY, Optional.of(input), new WalkedPath(), null, null);

    return input;
  }
}
