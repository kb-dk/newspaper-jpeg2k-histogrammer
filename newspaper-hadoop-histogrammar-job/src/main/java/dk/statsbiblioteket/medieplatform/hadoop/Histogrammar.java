package dk.statsbiblioteket.medieplatform.hadoop;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class Histogrammar {


    public static int[] computeHistogramValues(File filename) throws IOException {
        int[] result = new int[256];
        InputStream inputstream = getInputStream(filename);
        getHeader(inputstream);

        int colour;
        while ((colour = inputstream.read()) >= 0) {
            result[colour & 0xFF]++;
        }
        inputstream.close();
        return result;
    }

    private static InputStream getInputStream(File filename) throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(filename));
    }

    private static byte[] getHeader(InputStream stream) throws IOException {
        int value;
        int blocksFound = 0;
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        boolean isInWhiteSpace = false;
        while ((value = stream.read()) >= 0) {

            if (Character.isWhitespace(value)) {
                if (!isInWhiteSpace) {
                    isInWhiteSpace = true;
                    blocksFound++;
                }
            } else {
                isInWhiteSpace = false;
                if (blocksFound >= 4) {
                    break;
                }
            }
            header.write(value);
        }
        return header.toByteArray();

    }
}
