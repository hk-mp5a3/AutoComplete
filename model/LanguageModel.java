import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The LanguageModel class is used to extract language model 
 * results by MapReduce.
 */
public class LanguageModel {

    /**
     * The StartFollowMapper class is the mapper stage of Languge Model 
     * MapReduce. Find all the starting phrase(S) and following phrase(F)  
     * in each given N-Gram text. Extract KV<S, F=Count>.
     * For example, if the input value is like "president barack obama\t6"
     * The output will be like:
     * <"president", "barack obama=6">
     * <"president barack", "obama=6">
     * 
     * Input: (Key)LongWritable - can be ignored
     *        (Value)Text - N-gram text.
     * 
     * Output: (Key)Text - Starting phrase(S)
     *         (Value)Text - Following phrase(F) and its number of 
     *                       occurrences(Count). Formatted as "F=Count"
     */
    public static class StartFollowMapper
    extends Mapper < LongWritable, Text, Text, Text > {

        /**
         * A integer, threshold, used to filter phrases with low number 
         * of occurrences.
         */
        private int threshold;

        /**
         * A Text used to save the key of mapper output.
         */
        private Text word1 = new Text();

        /**
         * A Text used to save the value of mapper output.
         */
        private Text word2 = new Text();

        /**
         * Fetch parameter(s) in configuration.
         */
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold", 5);
        }

        public void map(LongWritable key, Text value, Context context)
        throws IOException,
        InterruptedException {
            // Check the input value is not empty
            if (value == null) {
                return;
            }

            // Split the input value to get N-Gram and count
            String[] splitedValues = value.toString().split("\t");
            String text = splitedValues[0].trim();
            String count = splitedValues[1].trim();

            // Check the text length is valid 
            if (text.length() == 0 || Integer.parseInt(count) < threshold) {
                return;
            }

            // Extract starting phrase, following phrase, and # of occurence
            int spaceIndex = text.lastIndexOf(" ");
            while (spaceIndex >= 0) {
                if (text.charAt(spaceIndex) == ' ') {
                    String startingWords = text.substring(0, spaceIndex);
                    String followingWord = text.substring(spaceIndex + 1,
                        text.length());
                    if (startingWords != null && startingWords.length() != 0 &&
                        followingWord != null && followingWord.length() != 0) {
                        StringBuilder wordAndCount;
                        wordAndCount = new StringBuilder();
                        wordAndCount.append(followingWord);
                        wordAndCount.append("=");
                        wordAndCount.append(count);
                        word1.set(startingWords);
                        word2.set(wordAndCount.toString());
                        context.write(word1, word2);
                    }
                }
                spaceIndex--;
            }
        }
    }

    /**
     * The ProbabilityReducer class is the reducer stage of Languge Model 
     * MapReduce. For each key(starting phrase), select K following phrase
     * with top number of occurences(highest conditional probability). 
     * 
     * Input: (Key)Text - Starting phrase(S)
     *        (Value)Text -  Following phrase(F) and its number of 
     *                       occurrences(Count). Formatted as "F=Count"
     * 
     * Output: (Key)DBOutput, - DBOutput object, with starting_words=S
     *                          following_word=F, word_count=Count    
     *         (Value)NullWritable - NullWritable object.
     */
    public static class ProbabilityReducer
    extends Reducer < Text, Text, DBOutput, NullWritable > {

        /**
         * A integer, K, used to select top K following phrases with 
         * top number of occurences(highest conditional probability). 
         */
        private int topK;

        /**
         * Fetch parameter(s) in configuration.
         */
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", 10);
        }

        public void reduce(Text key, Iterable < Text > values,
            Context context
        ) throws IOException,
        InterruptedException {
            // Sort following phrase by its number of occurences
            TreeMap < Integer, List < String >> sortedMap =
                new TreeMap < Integer, List < String >>
                (Collections.reverseOrder());
            for (Text value: values) {
                String[] splitedValue = value.toString().trim().split("=");
                String word = splitedValue[0];
                Integer count = Integer.parseInt(splitedValue[1]);

                if (sortedMap.containsKey(count)) {
                    sortedMap.get(count).add(word);
                } else {
                    List < String > newStrList = new ArrayList < String > ();
                    newStrList.add(word);
                    sortedMap.put(count, newStrList);
                }
            }

            /** 
             *  Extract K following phrase with top number of occurences
             *  (highest conditional probability) as the output of the reducer.
             */
            Iterator < Integer > iter = sortedMap.keySet().iterator();
            int outputCounter = 0;

            while (iter.hasNext() && outputCounter < topK) {
                int wordCount = iter.next();
                List < String > words = sortedMap.get(wordCount);
                for (String outputFollowingWord: words) {
                    context.write(new DBOutput(key.toString(),
                        outputFollowingWord, wordCount), NullWritable.get());
                    outputCounter++;
                }
            }
        }
    }
}