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

public class StringListFactory implements AccumulatorStateFactory {
  private static final long ARRAY_LIST_SIZE =
      ClassLayout.parseClass(ArrayList.class).instanceSize();

  @Override
  public Object createSingleState() {
    return new SingleStringListFactory();
  }

  @Override
  public Class getSingleStateClass() {
    return SingleStringListFactory.class;
  }

  @Override
  public Object createGroupedState() {
    return new GroupedStringListFactory();
  }

  @Override
  public Class getGroupedStateClass() {
    return GroupedStringListFactory.class;
  }

  public static class GroupedStringListFactory
      implements GroupedAccumulatorState, StringListState {
    private final ObjectBigArray<List<String>> lists = new ObjectBigArray<>();

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

    public List<String> getList() {
      if (lists.get(Math.toIntExact(groupId)) == null) {
        lists.set(groupId, new ArrayList<String>());
        memoryUsage += ARRAY_LIST_SIZE;
      }
      return lists.get(groupId);
    }

    @Override
    public long getEstimatedSize() {
      return memoryUsage;
    }

  }

  public static class SingleStringListFactory
      implements StringListState {
    private final List<String> list = new ArrayList<>();

    private int memoryUsage;

    public void addMemoryUsage(int memory) {
      memoryUsage += memory;
    }

    public List<String> getList() {
      return list;
    }


    @Override
    public long getEstimatedSize() {
      return memoryUsage + ARRAY_LIST_SIZE;
    }
  }
}
