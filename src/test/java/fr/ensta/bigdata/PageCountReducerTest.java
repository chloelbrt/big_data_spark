package fr.ensta.bigdata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class PageCountReducerTest {
    @SuppressWarnings("unchecked")
    @Test
    void testReduceOk() throws IOException, InterruptedException {
        // given
        var key = new Text("someWikiCode");
        var values = List.of(new IntWritable(1), new IntWritable(2));
        var reducer = new PageCountReducer();
        var context = mock(PageCountReducer.Context.class);

        // when
        reducer.reduce(key, values, context);

        // then
        verify(context).write(new Text("someWikiCode"), new IntWritable(3));
    }
}
