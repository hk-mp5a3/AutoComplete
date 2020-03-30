import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The ModelDriver class is used to run hadoop jobs to compute
 * the search suggestion based on given corpus.
 */
public class ModelDriver {

    /**
    * Build N-Gram language model and extract results to SQL database.
    *
    * @param  args   Array of Strings. It has order: 
    *                Input path, intermidiate output path for N-gram model,
    *                N of N-Gram, threshold, K of topK results
    * @return        None
    */
    public static void main(String[] args) throws Exception {
        /**
        * Part I:
        * Build N-Gram based on the given corpus.
        */
        Configuration conf1 = new Configuration();
        conf1.set("N", args[2]);

        Job ngramBuilderJob = Job.getInstance(conf1, "N-Gram Builder");
        ngramBuilderJob.setJarByClass(ModelDriver.class);

        ngramBuilderJob.setMapperClass(NGramBuilder.TokenizerMapper.class);
        ngramBuilderJob.setReducerClass(NGramBuilder.IntSumReducer.class);

        ngramBuilderJob.setOutputKeyClass(Text.class);
        ngramBuilderJob.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(ngramBuilderJob, new Path(args[0]));
        TextOutputFormat.setOutputPath(ngramBuilderJob, new Path(args[1]));

        ngramBuilderJob.waitForCompletion(true);

        /**
        * Part II:
        * Build language model and extract the result to SQL database.
        */
        Configuration conf2 = new Configuration();
        conf2.set("threshold", args[3]);
        conf2.set("topK", args[4]);
        DBConfiguration.configureDB(conf2,
            "com.mysql.jdbc.Driver",
            "jdbc:mysql://localhost:3306/ngram",
            "yzhu01",
            "password");

        Job languageModelJob = Job.getInstance(conf2, "Language Model");
        languageModelJob.setJarByClass(ModelDriver.class);
        languageModelJob.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.48-bin.jar"));
        
        languageModelJob.setMapperClass(LanguageModel.StartFollowMapper.class);
        languageModelJob.setReducerClass(LanguageModel.ProbabilityReducer.class);
        
        languageModelJob.setMapOutputKeyClass(Text.class);
        languageModelJob.setMapOutputValueClass(Text.class);
        
        languageModelJob.setOutputKeyClass(Text.class);
        languageModelJob.setOutputValueClass(NullWritable.class);
        
        languageModelJob.setInputFormatClass(TextInputFormat.class);
        languageModelJob.setOutputFormatClass(DBOutputFormat.class);
        
        DBOutputFormat.setOutput(
            languageModelJob,
            "auto_complete",
            new String[] {
                "starting_words",
                "following_word",
                "word_count"
            }
        );
        TextInputFormat.setInputPaths(languageModelJob, new Path(args[1]));
        
        System.exit(languageModelJob.waitForCompletion(true) ? 0 : 1);
    }
}