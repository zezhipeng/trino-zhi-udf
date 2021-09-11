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

package io.trino.plugin.zhi.word;

import static io.trino.plugin.zhi.word.WordTokenizeFunction.getNlpSeg;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.zhi.state.StringListState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarcharType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

//import io.airlift.log.Logger;

@AggregationFunction("word_count")
public class WordCountFunction {
  @InputFunction
  public static void input(StringListState state, @SqlType(StandardTypes.VARCHAR) Slice word) {
    if (null != word) {
      List<String> list = getNlpSeg(word.toStringUtf8());

      state.getList().addAll(list);
    }
  }

  @CombineFunction
  public static void combine(StringListState state, StringListState otherState) {
    int listSize = state.getList().size();
    int otherListSize = otherState.getList().size();

    if (listSize > 0 || otherListSize > 0) {
      state.getList().addAll(otherState.getList());
    }
  }

  @OutputFunction(
      "array(row(word " + StandardTypes.VARCHAR + ",pOs " + StandardTypes.VARCHAR + ",full_word " +
          StandardTypes.VARCHAR + ",count " + StandardTypes.DOUBLE + "))")
  public static void output(StringListState state, BlockBuilder out) {
    if (state.getList().isEmpty()) {
      out.appendNull();
    } else {
      List<String> list = state.getList();
      Map<String, Long> counted = list
          .stream()
          .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

      BlockBuilder blockBuilder = out.beginBlockEntry();

      counted.forEach((key, val) -> {
        BlockBuilder rowKeyBuilder = blockBuilder.beginBlockEntry();
        List<String> array = Arrays.asList(key.split("/", 2));

        VarcharType.VARCHAR.writeSlice(rowKeyBuilder, Slices.utf8Slice(array.get(0)));

        if (array.size() == 1) {
          VarcharType.VARCHAR.writeSlice(rowKeyBuilder, Slices.utf8Slice(""));
        } else {
          VarcharType.VARCHAR.writeSlice(rowKeyBuilder, Slices.utf8Slice(array.get(1)));
        }
        VarcharType.VARCHAR.writeSlice(rowKeyBuilder, Slices.utf8Slice(key));
        DoubleType.DOUBLE.writeDouble(rowKeyBuilder, val);

        blockBuilder.closeEntry();
      });

      out.closeEntry();
    }
  }
}
