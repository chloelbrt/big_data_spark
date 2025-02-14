package fr.ensta.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class PageCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    // Record class to encapsulate parsed information
    public record Record(
        LocalDate date,
        String wikiCode,
        String articleTitle,
        String pageId,
        String tag,
        Integer dailyTotal,
        Map<Integer, Integer> hourlyCount) {
        
        // Parses hourly view counts from a given input string
        public static Map<Integer, Integer> parseHourly(String input) {
            var map = new HashMap<Integer, Integer>();
            int previous = 0;
            int currentHour = -1;
            for (int current = 0; current < input.length(); current++) {
                var currentChar = input.charAt(current);
                if ('A' <= currentChar && currentChar <= 'Z') {
                    if (current > previous) {
                        var countStr = input.substring(previous, current);
                        var count = Integer.parseInt(countStr);
                        map.put(currentHour, count);
                    }
                    currentHour = currentChar - 'A';
                    previous = current + 1;
                }
            }
            // Handle remaining data for the last hour
            if (previous < input.length()) {
                var countStr = input.substring(previous);
                var count = Integer.parseInt(countStr);
                map.put(currentHour, count);
            }
            return map;
        }

        // Parses a line with a specific date and returns a Record instance
        public static Record parseWithDate(LocalDate date, String line) {
            String[] parts = line.split(" ");
            if (parts.length < 6) {
                throw new IllegalArgumentException("Invalid line format: " + line);
            }
            String wikiCode = parts[0];
            String articleTitle = parts[1];
            String pageId = parts[2];
            String tag = parts[3];
            int viewCount = Integer.parseInt(parts[4]);
            // Parse additional data for hourly counts
            String data = parts[5];
            Map<Integer, Integer> additionalData = parseHourly(data);
            return new Record(date, wikiCode, articleTitle, pageId, tag, viewCount, additionalData);
        }
    }

    public static LocalDate parseInputFileDate(Path path) {
        String fileName = path.getName();
        String yearStr = fileName.substring(10,14);
        Integer year = Integer.parseInt(yearStr);
        String monthStr = fileName.substring(14,16);
        Integer month = Integer.parseInt(monthStr);
        String dayStr = fileName.substring(16,18);
        Integer day = Integer.parseInt(dayStr);
        LocalDate date = LocalDate.of(year,month,day);
        return date;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Record record = Record.parseWithDate(null, value.toString());
        context.write(new Text(record.wikiCode()), new IntWritable(record.dailyTotal()));
    }
}