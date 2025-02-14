package fr.ensta.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /* START STUDENT CODE */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();  // Addition des valeurs
        }
        context.write(key, new IntWritable(sum));  // Écriture du résultat
    }
    /* END STUDENT CODE */
}
