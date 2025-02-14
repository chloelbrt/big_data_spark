package fr.ensta.bigdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;


public class PageCountMapperTest {
    @Test
    void parseRecordOk() {
        // given
        String line = "someWikiCode someArticleTitle somePageId someTag 4 A1C3";

        // when
        var parsed = PageCountMapper.Record.parseWithDate(null, line);

        // then
        var expected = new PageCountMapper.Record(
                null,
                "someWikiCode",
                "someArticleTitle",
                "somePageId",
                "someTag",
                4,
                Map.of(0, 1, 2, 3));
        assertEquals(expected, parsed);
    }

    @Test
    void testParseDateOk() {
        // given
        var path = new Path("data/pageviews-20240101-user.bz2");

        // when
        var parsed = PageCountMapper.parseInputFileDate(path);

        // then
        var expected = LocalDate.of(2024, 1, 1);
        assertEquals(expected, parsed);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testMapOk() throws IOException, InterruptedException {
        // given
        var line = "someWikiCode someArticleTitle somePageId someTag 4 A1C3";
        var mapper = new PageCountMapper();
        var context = mock(PageCountMapper.Context.class);

        // when
        mapper.map(null, new Text(line), context);

        // then
        verify(context).write(new Text("someWikiCode"), new IntWritable(4));
    }

}
