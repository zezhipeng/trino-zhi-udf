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

import io.trino.plugin.zhi.state.DoubleAndLongListState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import java.util.ArrayList;
import java.util.List;


@AggregationFunction("funnel_merge")
public class FunnelMerge {
  public static List<Long> buildList(Long size) {
    List<Long> list = new ArrayList<>();
    for (int i = 0; i < size; ++i) {
      list.add(1L);
    }

    return list;
  }

  public static List<Long> sumList(List<Long> left, List<Long> right) {
    List<Long> res = new ArrayList<>();
    for (int i = 0; i < Math.max(left.size(), right.size()); ++i) {
      long leftVal = i > left.size() - 1 ? 0 : left.get(i);
      long rightRight = i > right.size() - 1 ? 0 : right.get(i);
      res.add(leftVal + rightRight);
    }

    return res;
  }

  @InputFunction
  public static void input(DoubleAndLongListState state,
                           @SqlType(StandardTypes.BIGINT) long count) {
    List<Long> list = buildList(count);
    List<Long> latestList = state.getLongList();
    List<Long> res = sumList(list, latestList);

    state.setLongList(res);
  }

  @CombineFunction
  public static void combine(DoubleAndLongListState state, DoubleAndLongListState otherState) {
    List<Long> left = state.getLongList();
    List<Long> right = otherState.getLongList();

    List<Long> res = sumList(left, right);

    state.setLongList(res);
  }

  @OutputFunction("array(" + StandardTypes.BIGINT + ")")
  public static void output(DoubleAndLongListState state, BlockBuilder out) {
    List<Long> res = state.getLongList();
    BlockBuilder block = out.beginBlockEntry();

    for (long val : res) {
      BigintType.BIGINT.writeLong(block, val);
    }

    out.closeEntry();
  }
}
