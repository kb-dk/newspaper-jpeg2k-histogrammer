package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorSchemeType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorsType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.HistogramType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ObjectFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

/**
 * Input is line-number, line text. The text is the path to a file to run jpylyzer on
 * Output is line text, jpylyzer output xml
 */
public class PgmToHistogramMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger log = Logger.getLogger(PgmToHistogramMapper.class);
    private ObjectFactory factory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        factory = new ObjectFactory();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            File pgmPath = new File(value.toString());
            String histogram = histogram(pgmPath);
            boolean deleted = pgmPath.delete();

            context.write(value, new Text(histogram));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    public String histogram(File pgmPath) throws IOException {

        int[] histogramData = Histogrammar.computeHistogramValues(pgmPath);

        HistogramType histogram = factory.createHistogramType();
        ColorsType colors = factory.createColorsType();
        List<ColorType> colorList = colors.getColor();
        for (int code = 0; code < histogramData.length; code++) {
            int count = histogramData[code];
            ColorType color = factory.createColorType();
            color.setCode(code);
            color.setCount(count);
            colorList.add(color);
        }
        histogram.setColors(colors);
        ColorSchemeType scheme = getColorSchemeType(factory);
        histogram.setColorScheme(scheme);
        return toXML(histogram);
    }

    private ColorSchemeType getColorSchemeType(ObjectFactory factory) {
        ColorSchemeType scheme = factory.createColorSchemeType();
        scheme.setColorDepth("8 bits");
        scheme.setColorSpace("Greyscale");
        return scheme;
    }

    /**
     * Get the premis as xml
     *
     * @return the premis as xml
     */
    public String toXML(HistogramType histogram) {
        try {
            JAXBContext context = JAXBContext.newInstance(ObjectFactory.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            StringWriter writer = new StringWriter();
            marshaller.marshal(factory.createHistogram(histogram), writer);
            return writer.toString();
        } catch (JAXBException e) {
            return null;
        }
    }

}
