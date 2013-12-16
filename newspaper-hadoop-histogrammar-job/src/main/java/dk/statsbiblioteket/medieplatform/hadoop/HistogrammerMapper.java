package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorSchemeType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ColorsType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.HistogramType;
import dk.statsbiblioteket.medieplatform.hadoop.histogram.ObjectFactory;
import dk.statsbiblioteket.util.console.ProcessRunner;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Input is line-number, line text. The text is the path to a file to run jpylyzer on
 * Output is line text, jpylyzer output xml
 */
public class HistogrammerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger log = Logger.getLogger(HistogrammerMapper.class);

    /**
     * run command on the given file
     * @param dataPath     the path to the jp2 file
     * @param commandPath the path to the executable
     *
     * @return the path to the converted file
     * @throws java.io.IOException if the execution of jpylyzer failed in some fashion (not invalid file, if the program
     *                     returned non-zero returncode)
     */
    protected static String convert(String dataPath, String commandPath) throws IOException {
        ProcessRunner runner = new ProcessRunner(commandPath, dataPath);
        log.info("Running command '" + commandPath + " " + dataPath + "'");
        Map<String, String> myEnv = new HashMap<String, String>(System.getenv());
        runner.setEnviroment(myEnv);
        runner.setOutputCollectionByteSize(Integer.MAX_VALUE);

        String resultPath = getConvertedPath(dataPath);

        //this call is blocking
        runner.run();

        //we could probably do something more clever with returning the output while the command is still running.
        if (runner.getReturnCode() == 0) {
            return resultPath;
        } else {
            String message
                    = "failed to run jpylyzer, returncode:" + runner.getReturnCode() + ", stdOut:" + runner.getProcessOutputAsString() + " stdErr:" + runner.getProcessErrorAsString();
            log.error(message);
            throw new IOException(message);
        }
    }

    private static String getConvertedPath(String dataPath) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String commandPath = context.getConfiguration()
                                         .get(ConfigConstants.JPYLYZER_PATH);
            String pgmPath = convert(value.toString(), commandPath);

            String histogram = histogram(pgmPath);

            boolean deleted = delete(pgmPath);

            context.write(value, new Text(histogram));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Delete file where it is found
     * @param pgmPath
     */
    private boolean delete(String pgmPath) {
        return new File(pgmPath).delete();

    }

    private String histogram(String pgmPath) throws IOException {

        long[] histogramData = Historamer.computeHistogramValues(new File(pgmPath));
        ObjectFactory factory = new ObjectFactory();
        HistogramType histogram = factory.createHistogramType();
        ColorsType colors = factory.createColorsType();
        List<ColorType> colorList = colors.getColor();
        for (int code = 0; code < histogramData.length; code++) {
            long count = histogramData[code];
            ColorType color = factory.createColorType();
            color.setCode(code);
            color.setCount((int) count);
            colorList.add(color);
        }
        histogram.setColors(colors);
        ColorSchemeType scheme = factory.createColorSchemeType();
        scheme.setColorDepth("8 bits");
        scheme.setColorSpace("Greyscale");
        histogram.setColorScheme(scheme);
        return toXML(histogram);
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
              marshaller.marshal(histogram, writer);
              return writer.toString();
          } catch (JAXBException e) {
              return null;
          }
      }

}
