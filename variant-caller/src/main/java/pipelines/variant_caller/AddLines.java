package pipelines.variant_caller;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


public class AddLines extends CombineFn<String, AddLines.Accum, String> {
  
  private static final long serialVersionUID = 1L;
  //private static final Logger LOG = LoggerFactory.getLogger(AddLines.class);

  public static class Accum implements Serializable {
    private static final long serialVersionUID = 1L;
    String line;
    final String seed;
    
    public Accum(String seed, String line) {
      this.line = line;
      this.seed = seed;
    }
    
    public static Coder<Accum> getCoder() {
      return new AtomicCoder<Accum>() {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        @Override
        public void encode(Accum accumulator, OutputStream outStream) throws IOException {
          StringUtf8Coder.of().encode(accumulator.seed, outStream);
          StringUtf8Coder.of().encode(accumulator.line, outStream);
        }

        @Override
        public Accum decode(InputStream inStream) throws IOException {
          String seed = StringUtf8Coder.of().decode(inStream);
          String value = StringUtf8Coder.of().decode(inStream);
          return new Accum(seed, value);
        }
      };
    }
  }

  @Override
  public Coder<Accum> getAccumulatorCoder(
      CoderRegistry registry, Coder<String> inputCoder) {
    return Accum.getCoder();
  }
  
  @Override
  public Accum createAccumulator() { return new Accum("", ""); }

  @Override
  public Accum addInput(Accum accum, String line) {
      accum.line = line;
      return accum;
  }
  
  @Override
  public Accum mergeAccumulators(Iterable<Accum> accums) {
    Accum merged = createAccumulator();
    for (Accum accum : accums) {
      if (accum.line.length() > 1)
        merged.line = merged.line.concat("\n").concat(accum.line);
    }
    return merged;
  }
  
  @Override
  public String extractOutput(Accum accum) {
    return accum.line;
  }

}
