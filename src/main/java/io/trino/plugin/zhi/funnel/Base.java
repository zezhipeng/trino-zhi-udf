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

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Base {
  // 漏斗事件和索引关系: { events: { event index, ... } ... }
  public static Map<Slice, Map<Slice, Byte>> event_pos_dict = new HashMap<>();
  public static Map<String, Integer> stepMap = new HashMap<>();

  public static byte intToByte(int x) {
    return (byte) x;
  }

  public static int byteToInt(byte b) {
    return b & 0xFF;
  }

  // v2
  public static void setFunnelSteps(Block... steps) {
    // 转换成倒排索引
    // {eventName -> 1, eventName2 -> 1, eventName3 -> 2}
    int i = 0;
    for (Block step : steps) {
      int positionCount = step.getPositionCount();
      for (int x = 0; x < positionCount; x++) {
        Slice stepItem = (Slice) readNativeValue(VARCHAR, step, x);
        assert stepItem != null;
        String stepItemString = stepItem.toStringUtf8();
        stepMap.put(stepItemString, i);
      }
      ++i;
    }
  }

  // v1
  public static void init_events(Slice events) {
    try {
      List<String> fs =
          Arrays.asList(new String(events.getBytes(), StandardCharsets.UTF_8).split(","));

      Map<Slice, Byte> pos_dict = new HashMap<>();

      for (int i = 0; i < fs.size(); ++i) {
        pos_dict.put(Slices.utf8Slice(fs.get(i)), intToByte(i));
      }
      event_pos_dict.put(events, pos_dict);
    } catch (ArrayIndexOutOfBoundsException e) {
      System.out.println(e);
    }
  }
}
