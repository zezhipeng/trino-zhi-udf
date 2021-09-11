/*
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

package io.trino.plugin.zhi.state;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import java.util.ArrayList;
import java.util.List;
import org.openjdk.jol.info.ClassLayout;

public class DoubleAndLongListFactory implements AccumulatorStateFactory {
  private static final long ARRAY_LIST_SIZE =
      ClassLayout.parseClass(ArrayList.class).instanceSize();

  @Override
  public Object createSingleState() {
    return new SingleDoubleAndLongListFactory();
  }

  @Override
  public Class getSingleStateClass() {
    return SingleDoubleAndLongListFactory.class;
  }

  @Override
  public Object createGroupedState() {
    return new GroupedDoubleAndLongListFactory();
  }

  @Override
  public Class getGroupedStateClass() {
    return GroupedDoubleAndLongListFactory.class;
  }

  public static class GroupedDoubleAndLongListFactory
      implements GroupedAccumulatorState, DoubleAndLongListState {
    private final ObjectBigArray<List<Double>> lists = new ObjectBigArray<>();
    private final ObjectBigArray<List<Long>> longLists = new ObjectBigArray<>();

    private long memoryUsage;
    private long groupId;

    @Override
    public void setGroupId(long groupId) {
      this.groupId = groupId;
    }


    public void ensureCapacity(long size) {
      lists.ensureCapacity(Math.toIntExact(size));
    }

    public void addMemoryUsage(int memory) {
      memoryUsage += memory;
    }

    public List<Double> getDoubleList() {
      if (lists.get(Math.toIntExact(groupId)) == null) {
        lists.set(groupId, new ArrayList<Double>());
        memoryUsage += ARRAY_LIST_SIZE;
      }
      return lists.get(groupId);
    }

    public List<Long> getLongList() {
      if (longLists.get(Math.toIntExact(groupId)) == null) {
        longLists.set(groupId, new ArrayList<Long>());
        memoryUsage += ARRAY_LIST_SIZE;
      }
      return longLists.get(groupId);
    }

    public void setLongList(List<Long> list) {
      if (longLists.get(Math.toIntExact(groupId)) == null) {
        longLists.set(groupId, list);
        memoryUsage += ARRAY_LIST_SIZE;
      }
    }

    @Override
    public long getEstimatedSize() {
      return memoryUsage;
    }

  }

  public static class SingleDoubleAndLongListFactory
      implements DoubleAndLongListState {
    private List<Double> list = new ArrayList<>();
    private List<Long> longList = new ArrayList<>();

    private int memoryUsage;

    public void addMemoryUsage(int memory) {
      memoryUsage += memory;
    }

    public List<Double> getDoubleList() {
      return list;
    }

    public void setDoubleList(List<Double> _list) {
      list = _list;
    }

    public List<Long> getLongList() {
      return longList;
    }

    public void setLongList(List<Long> list) {
      longList = list;
    }


    @Override
    public long getEstimatedSize() {
      return memoryUsage + ARRAY_LIST_SIZE;
    }
  }
}
