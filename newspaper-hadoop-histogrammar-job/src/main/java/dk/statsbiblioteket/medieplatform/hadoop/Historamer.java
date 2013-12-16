package dk.statsbiblioteket.medieplatform.hadoop;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class Historamer {

   /* public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        long[] imagedata = read_pgm_img_data(new File(args[0]));
        System.out.println(Arrays.toString(imagedata));
        System.out.println(System.currentTimeMillis()-start);

    }
*/
    public static long[] computeHistogramValues(File filename) throws IOException {
        //Return image data from a raw PGM file as a numpy array, stripping the header information

        long[] result = new long[256];
        InputStream inputstream = getInputStream(filename);
        getHeader(inputstream);

        int colour;
        while ((colour = inputstream.read()) >= 0){
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
        while ((value = stream.read()) >= 0){

            if (Character.isWhitespace(value)){
                if (!isInWhiteSpace){
                    isInWhiteSpace = true;
                    blocksFound ++;
                }
            } else {
                isInWhiteSpace = false;
                if (blocksFound >= 4){
                    break;
                }
            }
            header.write(value);
            System.out.print(Character.toChars(value));
        }
        return header.toByteArray();

    }
}
