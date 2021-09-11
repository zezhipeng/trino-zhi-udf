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

package io.trino.plugin.zhi.utils;

import static io.trino.spi.type.TypeUtils.readNativeValue;

import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import java.util.ArrayList;
import java.util.List;

public class ArrayUtils {
  private ArrayUtils() {
  }

  public static List<Long> blockListLongOf(Block block) {
    List<Long> ret = new ArrayList<Long>();
    int positionCount = block.getPositionCount();
    for (int i = 0; i < positionCount; i++) {
      ret.add((Long) readNativeValue(BigintType.BIGINT, block, i));
    }
    return ret;
  }

  public static String ListLongStringOf(List<Long> list) {
    StringBuilder sb = new StringBuilder();
    list.forEach(item -> sb.append(item).append("|"));
    return sb.toString();
  }

  public static String ListStringOf(List<String> list) {
    StringBuilder sb = new StringBuilder();
    list.forEach(item -> sb.append(item).append("|"));
    return sb.toString();
  }

  public static String ListIntegerStringOf(List<Integer> list) {
    StringBuilder sb = new StringBuilder();
    list.forEach(item -> sb.append(item).append("|"));
    return sb.toString();
  }
}
