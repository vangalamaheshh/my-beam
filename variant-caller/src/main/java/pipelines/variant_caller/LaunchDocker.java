package pipelines.variant_caller;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.transforms.DoFn;

public class LaunchDocker extends DoFn<String, String> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AddLines.class);
  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    // Get the input element from ProcessContext.
    String word = c.element().split(",")[0];
    ProcessBuilder pb = new
        ProcessBuilder("/bin/bash", "-c",
     "docker run --rm ubuntu:16.04 sleep 20");
     final Process p = pb.start();
     BufferedReader br=new BufferedReader(
         new InputStreamReader(
            p.getInputStream()));
            String line;
            while((line=br.readLine())!=null) {
               LOG.warn(line);
            }
    // Use ProcessContext.output to emit the output element.
    c.output(word);
  }
}
