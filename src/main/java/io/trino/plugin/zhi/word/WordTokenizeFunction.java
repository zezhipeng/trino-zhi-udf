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


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import java.util.ArrayList;
import java.util.List;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;

public final class WordTokenizeFunction {
  @ScalarFunction("word_tokenize")
  @Description("Returns the stem of a word in the Chinese language")
  @SqlType(StandardTypes.VARCHAR)
  public static Slice wordTokenize(
      @SqlType(StandardTypes.VARCHAR) Slice slice) {
    return Slices.utf8Slice(getNlpSeg(slice.toStringUtf8()).toString());
  }

  public static List<String> getNlpSeg(String text) {
    List<String> words = new ArrayList<>();
    Result parse = NlpAnalysis.parse(text);
    for (Term term : parse) {
      if (null != term.getName() && !term.getName().trim().isEmpty()) {
        words.add(term.toString());
      }
    }
    return words;
  }
}
