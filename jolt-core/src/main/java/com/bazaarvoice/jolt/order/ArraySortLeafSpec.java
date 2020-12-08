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

import com.bazaarvoice.jolt.common.tree.MatchedElement;
import com.bazaarvoice.jolt.common.tree.WalkedPath;
import com.bazaarvoice.jolt.exception.TransformException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Leaf level ArraySortSpec object.
 *
 * <p>If this ArraySortSpec's PathElement matches the input (successful parallel tree walk) this
 * ArraySortSpec has the information needed to sort the given data.
 */
public class ArraySortLeafSpec extends ArraySortSpec {

  private List<SortingInfo> sortingSpec = new ArrayList<>();

  public ArraySortLeafSpec(String rawKey, Map<String, Object> rhs) {
    super(rawKey);
    List<String> sortBy = handleIt(rhs.get("sortBy"));
    List<String> orderBy = handleIt(rhs.get("direction"));
    if (Objects.nonNull(sortBy) && !sortBy.isEmpty()) {
      for (int i = 0; i < sortBy.size(); i++) {
        String sort = sortBy.get(i);
        String order = orderBy.size() > i ? orderBy.get(i) : "asc";
        sortingSpec.add(new SortingInfo(sort, order));
      }
    }
  }

  private List<String> handleIt(Object object) {
    if (Objects.isNull(object)) {
      return new ArrayList<>();
    }
    if (object instanceof List) {
      for (Object item : (List) object) {
        if (!(item instanceof String)) {
          throw new RuntimeException("Some of value is not String");
        }
      }
      return (List<String>) object;
    }
    return null;
  }

  /**
   * If this ArraySortSpec matches the inputkey, then sort the data and return true.
   *
   * @return true if this this spec "handles" the inputkey such that no sibling specs need to see it
   */
  @Override
  public boolean applySorting(
      String inputKey, Object input, WalkedPath walkedPath, Object parentContainer) {

    MatchedElement thisLevel = getMatch(inputKey, walkedPath);
    if (thisLevel == null) {
      return false;
    }
    performSorting(inputKey, input, walkedPath, (Map) parentContainer, thisLevel);
    return true;
  }

  /**
   * This should only be used by composite specs with an '@' child
   *
   * @return null if no work was done, otherwise returns the re-parented data
   */
  public Object applyToParentContainer(
      String inputKey, Object input, WalkedPath walkedPath, Object parentContainer) {

    MatchedElement thisLevel = getMatch(inputKey, walkedPath);
    if (thisLevel == null) {
      return null;
    }
    return performSorting(inputKey, input, walkedPath, (Map) parentContainer, thisLevel);
  }

  /**
   * @return null if no work was done, otherwise returns the re-parented data
   * */
  private Object performSorting(
      String inputKey,
      Object input,
      WalkedPath walkedPath,
      Map parentContainer,
      MatchedElement thisLevel) {

    // Add our the LiteralPathElement for this level, so that write path References can use it as
    // &(0,0)
    walkedPath.add(input, thisLevel);
    Object returnValue = null;
    if (input instanceof List) {
      List list = (List) input;
      if (list.size() > 1) {
        if (list.get(0) instanceof Map) {
          list.sort(
              (x, y) -> {
                Map objectX = (Map) x;
                Map objectY = (Map) y;
                return compare(objectX, objectY);
              });
        } else {
          Collections.sort(list);
        }
        returnValue = new ArrayList(list);
      }
      parentContainer.put(inputKey, returnValue);
    }

    walkedPath.removeLast();

    return returnValue;
  }

  private int compare(Map obj1, Map obj2) {
    for (SortingInfo sortInfo : sortingSpec) {
      int positive = sortInfo.getOrderDirection().equals("desc") ? -1 : 1;
      Comparable value1 = getComparableValueFromMap(obj1, sortInfo);
      Comparable value2 = getComparableValueFromMap(obj2, sortInfo);
      if (Objects.nonNull(value1)) {
        if (Objects.isNull(value2)) {
          return 1 * positive;
        } else {
          if (value1.compareTo(value2) != 0) {
            return value1.compareTo(value2) * positive;
          }
        }
      } else {
        return -1 * positive;
      }
    }
    return 0;
  }

  private Comparable getComparableValueFromMap(Map object, SortingInfo sortInfo) {
    try {
      List<String> partsOfPath = Arrays.asList(sortInfo.getSortBy().split("\\."));
      return (Comparable) getInnerValue(object, partsOfPath);
    } catch (Exception e) {
      e.printStackTrace();
      throw new TransformException(
          "SortingTransform expect primitive types as criteria for sorting.");
    }
  }

  private Object getInnerValue(Map object, List<String> partsOfPath) {
    Object value = object.get(partsOfPath.get(0));
    if (partsOfPath.size() == 1) {
      return value;
    } else {
      if (!(value instanceof Map)) {
        return null;
      }
      List<String> parts = new ArrayList<>(partsOfPath);
      parts.remove(0);
      return getInnerValue((Map) value, parts);
    }
  }

  private MatchedElement getMatch(String inputKey, WalkedPath walkedPath) {
    return pathElement.match(inputKey, walkedPath);
  }

  public static class SortingInfo {

    private String sortBy;
    private String orderDirection;

    public SortingInfo(String sortBy, String orderDirection) {
      this.sortBy = sortBy;
      this.orderDirection = orderDirection;
    }

    public String getSortBy() {
      return sortBy;
    }

    public String getOrderDirection() {
      return orderDirection;
    }
  }
}
