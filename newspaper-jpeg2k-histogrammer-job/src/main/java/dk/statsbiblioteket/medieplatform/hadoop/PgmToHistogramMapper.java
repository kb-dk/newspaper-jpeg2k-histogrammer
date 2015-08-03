package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorSchemeType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorsType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.HistogramType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ObjectFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

/**
 * Input is jpeg2k path, pgm path.
 * Output is jpeg2k path, histogram xml
 */
public class PgmToHistogramMapper extends Mapper<Text, Text, Text, Text> {

    private static Logger log = Logger.getLogger(PgmToHistogramMapper.class);
    private ObjectFactory factory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        factory = new ObjectFactory();
    }


    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            log.debug("Mapping for '" + key + "' and '" + value + "'");
            File pgmPath = new File(value.toString());
            String histogram = histogram(pgmPath);
            boolean deleted = pgmPath.delete();
            if (!deleted) {
                log.warn("Failed to delete file '" + pgmPath + "', it will be left on the node");
            }
            context.write(key, new Text(histogram));
        } catch (Exception e) {
            log.error(e);
            throw new IOException(e);
        }
    }


    public String histogram(File pgmPath) throws Exception {

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
    public String toXML(HistogramType histogram) throws Exception {
        JAXBContext context = JAXBContext.newInstance(ObjectFactory.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
        StringWriter writer = new StringWriter();
        marshaller.marshal(factory.createHistogram(histogram), writer);
        return writer.toString();
    }

}
