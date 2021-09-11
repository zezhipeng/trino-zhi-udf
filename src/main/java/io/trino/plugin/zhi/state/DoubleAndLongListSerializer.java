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

import static io.trino.spi.type.VarbinaryType.VARBINARY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import java.io.IOException;
import java.util.List;

public class DoubleAndLongListSerializer
    implements AccumulatorStateSerializer<DoubleAndLongListState> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

  @Override
  public Type getSerializedType() {
    return VARBINARY;
  }

  public void serialize(DoubleAndLongListState state, BlockBuilder out) {
    if (state.getLongList() == null) {
      out.appendNull();
    } else {
      try {
        VARBINARY.writeSlice(out,
            Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(state.getLongList())));
      } catch (JsonProcessingException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public void deserialize(Block block, int index, DoubleAndLongListState state) {
    if (!block.isNull(index)) {
      SliceInput slice = VARBINARY.getSlice(block, index).getInput();
      List<Long> listState;
      try {
        listState = OBJECT_MAPPER.readValue(slice.readSlice(slice.available()).getBytes(),
            new TypeReference<List<Long>>() {
            });
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      state.getLongList().clear();
      state.getLongList().addAll((List<Long>) listState);
    }
  }
}
