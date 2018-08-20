package pipelines.variant_caller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.transforms.DoFn;

public class LaunchDocker {
  public static class LaunchJobs extends DoFn<String, String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LaunchJobs.class);
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      // Get the input element from ProcessContext.
      String word = c.element().split(",")[0];
      LOG.info(word);
      ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c",
          "docker run --rm ubuntu:16.04 sleep 20");
       pb.inheritIO().start().waitFor();
      // Use ProcessContext.output to emit the output element.
      if (!word.isEmpty())
        c.output(word + "\n");
    }
  }
}
