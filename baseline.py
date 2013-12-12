import re
import numpy
import array


def read_pgm_img_data(filename, byteorder='>'):
    """Return image data from a raw PGM file as a numpy array, stripping the header information"""
    with open(filename, 'rb') as f:
        buffer = f.read()
    try:
        header, width, height, maxval = re.search(
             b"(^P5\s(?:\s*#.*[\r\n])*"
             b"(\d+)\s(?:\s*#.*[\r\n])*"
             b"(\d+)\s(?:\s*#.*[\r\n])*"
             b"(\d+)\s(?:\s*#.*[\r\n]\s)*)", buffer).groups()
    except AttributeError:
        raise ValueError("Not a raw PGM file: '%s'" % filename)
     
    return numpy.frombuffer(buffer,
                             dtype='u1' if int(maxval) < 256 else byteorder+'u2',
                             count=int(width)*int(height),
                             offset=len(header)
                             )

if __name__ == "__main__":
    """from matplotlib import pyplot
    image = read_pgm("AutoBalance.pgm", byteorder='<')
    pyplot.imshow(image, pyplot.cm.gray)
    pyplot.show()"""
    

    imagedata = read_pgm_img_data("AutoBalance.pgm", byteorder='<')

    colours = array.array('l')
    for i in range(0,255): 
        colours.append(0)
        
    print colours    

    for i in range(0, len(imagedata)):
        colours[imagedata[i]] += 1
    
    print colours
