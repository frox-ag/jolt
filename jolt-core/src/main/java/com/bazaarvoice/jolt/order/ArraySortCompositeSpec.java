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

package com.bazaarvoice.jolt.order;

import com.bazaarvoice.jolt.common.ComputedKeysComparator;
import com.bazaarvoice.jolt.common.pathelement.AmpPathElement;
import com.bazaarvoice.jolt.common.pathelement.AtPathElement;
import com.bazaarvoice.jolt.common.pathelement.LiteralPathElement;
import com.bazaarvoice.jolt.common.pathelement.StarPathElement;
import com.bazaarvoice.jolt.common.tree.MatchedElement;
import com.bazaarvoice.jolt.common.tree.WalkedPath;
import com.bazaarvoice.jolt.exception.SpecException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** ArraySortSpec that has children, which it builds and then manages during Transforms. */
public class ArraySortCompositeSpec extends ArraySortSpec {

  private static final HashMap<Class, Integer> orderMap;
  private static final ComputedKeysComparator computedKeysComparator;

  static {
    orderMap = new HashMap<>();
    orderMap.put(AmpPathElement.class, 1);
    orderMap.put(StarPathElement.class, 2);
    computedKeysComparator = ComputedKeysComparator.fromOrder(orderMap);
  }

  // children that are simple exact matches against the input data
  private final Map<String, ArraySortSpec> literalChildren;

  // children that are regex matches against the input data
  private final List<ArraySortSpec> computedChildren;

  // children that aren't actually triggered off the input data
  private ArraySortLeafSpec specialChild;

  public ArraySortCompositeSpec(String rawKey, Map<String, Object> spec) {
    super(rawKey);

    Map<String, ArraySortSpec> literals = new HashMap<>();
    ArrayList<ArraySortSpec> computed = new ArrayList<>();

    specialChild = null;

    // self check
    if (pathElement instanceof AtPathElement) {
      throw new SpecException("@ ArraySortTransform key, can not have children.");
    }
    Map<String, Object> nextSpec = new LinkedHashMap<>(spec);
    List<ArraySortSpec> children = createChildren(nextSpec);

    if (children.isEmpty()) {
      throw new SpecException(
          "Shift ArraySortSpec format error : "
              + "ArraySortSpec line with empty {} as value is not valid.");
    }

    for (ArraySortSpec child : children) {
      literals.put(child.pathElement.getRawKey(), child);

      if (child.pathElement instanceof LiteralPathElement) {
        literals.put(child.pathElement.getRawKey(), child);
      } else if (child.pathElement instanceof AtPathElement) { // special is it is "@"
        if (child instanceof ArraySortLeafSpec) {
          specialChild = (ArraySortLeafSpec) child;
        } else {
          throw new SpecException("@ ArraySortTransform key, can not have children.");
        }
      } else { // star
        computed.add(child);
      }
    }

    // Only the computed children need to be sorted
    Collections.sort(computed, computedKeysComparator);

    computed.trimToSize();
    literalChildren = Collections.unmodifiableMap(literals);
    computedChildren = Collections.unmodifiableList(computed);
  }

  /** Recursively walk the spec input tree. */
  private static List<ArraySortSpec> createChildren(Map<String, Object> rawSpec) {

    List<ArraySortSpec> children = new ArrayList<>();
    Set<String> actualKeys = new HashSet<>();

    for (String keyString : rawSpec.keySet()) {

      Object rawRhs = rawSpec.get(keyString);

      ArraySortSpec childSpec;
      if (rawRhs instanceof Map) {
        Map<String, Object> spec = (Map<String, Object>) rawRhs;
        if (spec.containsKey("sortBy")) {
          childSpec = new ArraySortLeafSpec(keyString, spec);
        } else {
          childSpec = new ArraySortCompositeSpec(keyString, spec);
        }
      } else {
        continue;
      }

      String childCanonicalString = childSpec.pathElement.getCanonicalForm();

      if (actualKeys.contains(childCanonicalString)) {
        throw new IllegalArgumentException(
            "Duplicate canonical CardinalityTransform key found : " + childCanonicalString);
      }

      actualKeys.add(childCanonicalString);

      children.add(childSpec);
    }

    return children;
  }

  /**
   * This method implements the Cardinality matching behavior when we have both literal and computed
   * children.
   *
   * <p>For each input key, we see if it matches a literal, and it not, try to match the key with
   * every computed child.
   */
  private static void applyKeyToLiteralAndComputed(
      ArraySortCompositeSpec spec,
      String subKeyStr,
      Object subInput,
      WalkedPath walkedPath,
      Object input) {

    ArraySortSpec literalChild = spec.literalChildren.get(subKeyStr);

    // if the subKeyStr found a literalChild, then we do not have to try to match any of the
    // computed ones
    if (literalChild != null) {
      literalChild.applySorting(subKeyStr, subInput, walkedPath, input);
    } else {
      // If no literal spec key matched, iterate through all the computedChildren

      // Iterate through all the computedChildren until we find a match
      // This relies upon the computedChildren having already been sorted in priority order
      for (ArraySortSpec computedChild : spec.computedChildren) {
        // if the computed key does not match it will quickly return false
        if (computedChild.applySorting(subKeyStr, subInput, walkedPath, input)) {
          break;
        }
      }
    }
  }

  /**
   * If this Spec matches the inputkey, then perform one step in the parallel treewalk.
   *
   * <p>Step one level down the input "tree" by carefully handling the List/Map nature the input to
   * get the "one level down" data.
   *
   * <p>Step one level down the Spec tree by carefully and efficiently applying our children to the
   * "one level down" data.
   *
   * @return true if this this spec "handles" the inputkey such that no sibling specs need to see it
   */
  @Override
  public boolean applySorting(
      String inputKey, Object input, WalkedPath walkedPath, Object parentContainer) {
    MatchedElement thisLevel = pathElement.match(inputKey, walkedPath);
    if (thisLevel == null) {
      return false;
    }

    walkedPath.add(input, thisLevel);

    // The specialChild can change the data object that I point to.
    // Aka, my key had a value that was a List, and that gets changed so that my key points to a ONE
    // value
    if (specialChild != null) {
      input = specialChild.applyToParentContainer(inputKey, input, walkedPath, parentContainer);
    }

    // Handle the rest of the children
    process(input, walkedPath);

    walkedPath.removeLast();
    return true;
  }

  @SuppressWarnings("unchecked")
  private void process(Object input, WalkedPath walkedPath) {

    if (input instanceof Map) {

      // Iterate over the whole entrySet rather than the keyset with follow on gets of the values
      Set<Map.Entry<String, Object>> entrySet =
          new HashSet<>(((Map<String, Object>) input).entrySet());
      for (Map.Entry<String, Object> inputEntry : entrySet) {
        applyKeyToLiteralAndComputed(
            this, inputEntry.getKey(), inputEntry.getValue(), walkedPath, input);
      }
    } else if (input instanceof List) {

      for (int index = 0; index < ((List<Object>) input).size(); index++) {
        Object subInput = ((List<Object>) input).get(index);
        String subKeyStr = Integer.toString(index);

        applyKeyToLiteralAndComputed(this, subKeyStr, subInput, walkedPath, input);
      }
    } else if (input != null) {

      // if not a map or list, must be a scalar
      String scalarInput = input.toString();
      applyKeyToLiteralAndComputed(this, scalarInput, null, walkedPath, scalarInput);
    }
  }
}
