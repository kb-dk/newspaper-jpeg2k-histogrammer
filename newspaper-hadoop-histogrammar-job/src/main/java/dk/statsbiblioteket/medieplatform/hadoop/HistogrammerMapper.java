package dk.statsbiblioteket.medieplatform.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Input is line-number, line text. The text is the path to a file to run jpylyzer on
 * Output is line text, jpylyzer output xml
 */
public class HistogrammerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger log = Logger.getLogger(HistogrammerMapper.class);
    private Jp2ToPgmMapper mapper1;
    private PgmToHistogramMapper mapper2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mapper1 = new Jp2ToPgmMapper();
        mapper2 = new PgmToHistogramMapper();
        mapper1.setup(context);
        mapper2.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            File pgmPath = mapper1.convert(value.toString());
            String histogram = mapper2.histogram(pgmPath);
            pgmPath.delete();
            context.write(value,new Text(histogram));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }



}
