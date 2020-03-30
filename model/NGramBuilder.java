import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The NGramBuilder class is used to fetch N-Gram results by MapReduce.
 */
public class NGramBuilder {

    /**
     * The TokenizerMapper class is the reducer stage of N-Gram MapReduce.
     * Preprocesing the raw text
     *  - Trim
     *  - Keep only letters
     *  - Convert to lower cases
     *  - Remove the words with length 1
     *  - Remove the text with less than two words
     *  - Remove stop words
     * 
     * Input: (Key)Object - can be ignored
     *        (Value)Text - raw text from input files
     * 
     * Output: (Key)Text - N-Gram after preprocessing
     *         (Value)IntWritable - all are ones
     */
    public static class TokenizerMapper
    extends Mapper < Object, Text, Text, IntWritable > {

        /**
        * An Integer of N in N-Gram.
        */
        private int N;

        /**
        * A Text used to save the key of mapper output.
        */
        private Text word = new Text();

        /**
        * A IntWritable with value one used to save the 
        * value of mapper output. Constant.
        */
        private final static IntWritable ONE = new IntWritable(1);

        /**
        * A list of String with all the stop words. Constant.
        */
        private final static String STOP_WORDS_ARR[] = {
            "ourselves",
            "hers",
            "between",
            "yourself",
            "but",
            "again",
            "there",
            "about",
            "once",
            "during",
            "out",
            "very",
            "having",
            "with",
            "they",
            "own",
            "an",
            "be",
            "some",
            "for",
            "do",
            "its",
            "yours",
            "such",
            "into",
            "of",
            "most",
            "itself",
            "other",
            "off",
            "is",
            "s",
            "am",
            "or",
            "who",
            "as",
            "from",
            "him",
            "each",
            "the",
            "themselves",
            "until",
            "below",
            "are",
            "we",
            "these",
            "your",
            "his",
            "through",
            "don",
            "nor",
            "me",
            "were",
            "her",
            "more",
            "himself",
            "this",
            "down",
            "should",
            "our",
            "their",
            "while",
            "above",
            "both",
            "up",
            "to",
            "ours",
            "had",
            "she",
            "all",
            "no",
            "when",
            "at",
            "any",
            "before",
            "them",
            "same",
            "and",
            "been",
            "have",
            "in",
            "will",
            "on",
            "does",
            "yourselves",
            "then",
            "that",
            "because",
            "what",
            "over",
            "why",
            "so",
            "can",
            "did",
            "not",
            "now",
            "under",
            "he",
            "you",
            "herself",
            "has",
            "just",
            "where",
            "too",
            "only",
            "myself",
            "which",
            "those",
            "i",
            "after",
            "few",
            "whom",
            "t",
            "being",
            "if",
            "theirs",
            "my",
            "against",
            "a",
            "by",
            "doing",
            "it",
            "how",
            "further",
            "was",
            "here",
            "than"
        };

        /**
        * A hash set of String with all the stop words. Constant.
        */
        private final static Set < String > STOP_WORDS_SET =
        new HashSet < > (Arrays.asList(STOP_WORDS_ARR));

        /**
        * Fetch parameter(s) in configuration.
        */
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            N = conf.getInt("N", 5);
        }

        public void map(Object key, Text value, Context context) 
        throws IOException,
        InterruptedException {
            // Get text string
            String text = value.toString();

            // Trim, convert to lower cases, keep only letters
            text = text.trim();
            text = text.toLowerCase();
            text = text.replaceAll("[^a-z]", " ");
            String rawWords[] = text.split("\\s+");;

            // Remove the text with less than two words
            if (rawWords.length < 2) {
                return;
            }

            // Remove stop words and the words with length 1
            List < String > words = new ArrayList < String > ();
            for (String word: rawWords) {
                if (word.length() == 1) {
                    continue;
                }
                if (STOP_WORDS_SET.contains(word)) {
                    continue;
                }
                words.add(word);
            }

            // Construct Mapper output
            StringBuilder ngramStr;
            int word_length = words.size();
            for (int leftIndex = 0; leftIndex < word_length; leftIndex++) {
                ngramStr = new StringBuilder();
                ngramStr.append(words.get(leftIndex));
                for (int rightIndex = 1; 
                leftIndex + rightIndex < word_length &&
                    rightIndex < N; rightIndex++) {
                    ngramStr.append(" ");
                    ngramStr.append(words.get(leftIndex + rightIndex));
                    word.set(ngramStr.toString());
                    context.write(word, ONE);
                }
            }
        }
    }

    /**
     * The IntSumReducer class is the reducer stage of N-Gram MapReduce.
     * Count the occurrences of each key.
     * 
     * Input: (Key)Text - N-Gram generated from the raw text 
     *        (Value)Iterable < IntWritable > - all are ones
     * 
     * Output: (Key)Text - same as input 
     *         (Value)IntWritable - sum of values with the same key
     */
    public static class IntSumReducer
    extends Reducer < Text, IntWritable, Text, IntWritable > {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable < IntWritable > values,
            Context context
        ) throws IOException,
        InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}