package dk.statsbiblioteket.medieplatform.hadoop;

import dk.statsbiblioteket.medieplatform.autonomous.ConfigConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

public class KakaduConverterMapperTest {








    @Test
    public void testRealConvert() throws IOException, URISyntaxException {

        MapDriver<LongWritable, Text, Text, Text> mapDriver;
        String batchID = "B400022028241-RT1";

        ConvertMapper mapper = new ConvertMapper();
        String name = "B400022028241-RT1/balloon.jp2";
        String convertedFile = "/tmp/" + name + ".pgm";
        File resultFile = new File(convertedFile);
        resultFile.delete();

        mapDriver = MapDriver.newMapDriver(mapper);

        File testFolder = new File(Thread.currentThread().getContextClassLoader().getResource(
                name).toURI()).getParentFile().getParentFile().getParentFile().getParentFile();

        mapDriver.getConfiguration().set(ConfigConstants.HADOOP_CONVERTER_PATH, testFolder+"/src/test/extras/kakadu_run.sh kdu_expand -num_threads 1 -fprec 8M");
        mapDriver.getConfiguration().setIfUnset(ConfigConstants.BATCH_ID, batchID);
        mapDriver.getConfiguration().set(ConfigConstants.HADOOP_TEMP_PATH,"/tmp/");



        String testFile = getAbsolutePath(name);
        mapDriver.withInput(new LongWritable(1), new Text(testFile));

        mapDriver.withOutput(new Text(testFile), new Text(convertedFile));
        mapDriver.runTest();
        Assert.assertTrue(resultFile.exists());
        Assert.assertTrue(resultFile.isFile());
        resultFile.delete();

    }


    private String getAbsolutePath(String name) throws URISyntaxException {
        return new File(Thread.currentThread().getContextClassLoader().getResource(
                name).toURI()).getAbsolutePath();
    }

}
