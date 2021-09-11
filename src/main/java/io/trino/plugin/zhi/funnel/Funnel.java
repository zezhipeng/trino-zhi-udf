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

package io.trino.plugin.zhi.funnel;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.zhi.state.SliceState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AggregationFunction("funnel")
public class Funnel extends Base {
  private static final int COUNT_FLAG_LENGTH = 8 + 4;
  // 设置事件所占的位数,时间长和事件下标
  private static final int COUNT_ONE_LENGTH = 8 + 4;

  @CombineFunction
  public static void combine(
      SliceState state1,
      SliceState state2
  ) {
    Slice slice1 = state1.getSlice();
    Slice slice2 = state2.getSlice();

    if (slice2 == null) {
      return;
    }

    if (slice1 == null) {
      state1.setSlice(slice2);
    } else {
      int l1 = slice1.length();
      int l2 = slice2.length();
      Slice newSlice = Slices.allocate(l1 + l2 - COUNT_FLAG_LENGTH);
      newSlice.setBytes(0, slice1.getBytes());
      // 截取长度数据
      newSlice.setBytes(l1, slice2.getBytes(), COUNT_FLAG_LENGTH, l2 - COUNT_FLAG_LENGTH);
      state1.setSlice(newSlice);
    }
  }

  @OutputFunction(StandardTypes.BIGINT)
  public static void output(SliceState state, BlockBuilder out) {
    // 获取状态数据
    Slice slice = state.getSlice();

    if (slice == null) {
      out.appendNull();
      return;
    }

    boolean is_a = false;
    List<Double> time_array = new ArrayList<>();
    Map<Double, Integer> timeEventMap = new HashMap<>();

    int eventSize = slice.getInt(8);

    if (!is_a && eventSize == 0) {
      is_a = true;
    }

    for (int index = COUNT_FLAG_LENGTH; index < slice.length(); index += COUNT_ONE_LENGTH) {
      double timestamp = slice.getDouble(index);
      byte event_b = slice.getByte(index + 8);

      time_array.add(timestamp);
      timeEventMap.put(timestamp, (int) event_b);
    }
    if (is_a) {
      out.writeInt(0);
      out.closeEntry();
      return;
    }

    Collections.sort(time_array);

    double windows = slice.getDouble(0);
    long maxEventIndex = 0;

    List<double[]> temp = new ArrayList<>();

    for (double timestamp : time_array) {
      int event = timeEventMap.get(timestamp);
      if (event == 0) {
        double[] flag = {timestamp, event};
        temp.add(flag);
      } else {
        for (int i = temp.size() - 1; i >= 0; --i) {
          double[] flag = temp.get(i);
          if (timestamp - flag[0] >= windows) {
            break;
          } else {
            flag[1] = event;
            if (maxEventIndex < event) {
              maxEventIndex = event;
            }
          }
        }

        if ((maxEventIndex + 1) == eventSize) {
          break;
        }
      }
    }
    BigintType.BIGINT.writeLong(out, maxEventIndex + 1);
    out.closeEntry();
  }

  public static void inputBase(SliceState state, double eventTime, double windows, Slice event,
                               Block... steps) {
    Slice slice = state.getSlice();
    if (stepMap.size() == 0) {
      setFunnelSteps(steps);
    }
    String actionString = event.toStringUtf8();
    if (null == slice) {
      // 开辟空间
      slice = Slices.allocate(COUNT_FLAG_LENGTH + COUNT_ONE_LENGTH);
      // 窗口期
      slice.setDouble(0, (double) windows);
      // 事件个数
      slice.setInt(8, steps.length);

      // 保存事件发生的时间和对应的索引号
      // 事件对应索引
      if (stepMap.get(actionString) != null) {
        slice.setDouble(COUNT_FLAG_LENGTH, (double) eventTime);
        slice.setByte(20, stepMap.get(actionString));
      }
      state.setSlice(slice);
    } else {
      // 拿到上一个 slice 的长度
      // 新建 slice，并初始化，在上一个长度后面追加新的数据
      if (stepMap.get(actionString) != null) {
        int slice_len = slice.length();
        Slice newSlice = Slices.allocate(slice_len + COUNT_ONE_LENGTH);

        newSlice.setBytes(0, slice.getBytes());
        newSlice.setDouble(slice_len, (double) eventTime);
        newSlice.setByte(slice_len + 8, stepMap.get(actionString));

        state.setSlice(newSlice);
        // [win_size[8], event_size[4],event_time[8],event_idx[1],event_time[8],event_idx[1],event_time[8],event_idx[1]
      }
    }
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType(StandardTypes.VARCHAR) Slice events) {
    Slice slice = state.getSlice();
    if (!event_pos_dict.containsKey(events)) {
      init_events(events);
    }
    if (null == slice) {
      int event_size = event_pos_dict.get(events).size();
      // 开辟空间
      slice = Slices.allocate(COUNT_FLAG_LENGTH + COUNT_ONE_LENGTH);
      // 窗口期
      slice.setDouble(0, (double) windows);
      // 事件个数
      slice.setInt(8, event_pos_dict.get(events).size());

      // 保存事件发生的时间和对应的索引号
      // 事件对应索引
      if (event_pos_dict.get(events).get(event) != null) {
        slice.setDouble(COUNT_FLAG_LENGTH, (double) eventTime);
        slice.setByte(20, event_pos_dict.get(events).get(event));
      }
      state.setSlice(slice);
    } else {
      // 拿到上一个 slice 的长度
      // 新建 slice，并初始化，在上一个长度后面追加新的数据
      if (event_pos_dict.get(events).get(event) != null) {
        int slice_len = slice.length();
        Slice newSlice = Slices.allocate(slice_len + COUNT_ONE_LENGTH);

        newSlice.setBytes(0, slice.getBytes());
        newSlice.setDouble(slice_len, (double) eventTime);
        newSlice.setByte(slice_len + 8, event_pos_dict.get(events).get(event));

        state.setSlice(newSlice);
        // [win_size[8], event_size[4],event_time[8],event_idx[1],event_time[8],event_idx[1],event_time[8],event_idx[1]
      }
    }
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2) {
    inputBase(state, eventTime, windows, event, s1, s2);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3) {
    inputBase(state, eventTime, windows, event, s1, s2, s3);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4) {
    inputBase(state, eventTime, windows, event, s1, s2, s3, s4);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5) {
    inputBase(state, eventTime, windows, event, s1, s2, s3, s4, s5);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6) {
    inputBase(state, eventTime, windows, event, s1, s2, s3, s4, s5, s6);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7) {
    inputBase(state, eventTime, windows, event, s1, s2, s3, s4, s5, s6, s7);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8) {
    inputBase(state, eventTime, windows, event, s1, s2, s3, s4, s5, s6, s7, s8);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s9) {
    inputBase(state, eventTime, windows, event, s1, s2, s3, s4, s5, s6, s7, s8, s9);
  }

  @InputFunction
  public static void input(SliceState state,
                           @SqlType(StandardTypes.DOUBLE) double eventTime,
                           @SqlType(StandardTypes.DOUBLE) double windows,
                           @SqlType(StandardTypes.VARCHAR) Slice event,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s1,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s2,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s3,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s4,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s5,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s6,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s7,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s8,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s9,
                           @SqlType("array(" + StandardTypes.VARCHAR + ")") Block s10) {
    inputBase(state, eventTime, windows, event, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10);
  }
}
