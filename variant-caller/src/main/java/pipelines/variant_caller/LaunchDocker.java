package pipelines.variant_caller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.transforms.DoFn;

import pipelines.variant_caller.BeamKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;


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
          "source activate aws && export AWS_DEFAULT_PROFILE=mahesh_umass && " +
          "mkdir -p " + word + " && " +
          "aws s3 cp s3://synergist-prod-input/" + word + ".csv" + " " + word + "/" + word + ".csv && " +
          "docker run --rm -v /Users/vangalau/umms/git/my-beam/variant-caller/" + word + ":/mnt" + " ubuntu:16.04 " + 
          "mv /mnt/" + word + ".csv /mnt/out.csv; sleep 20" +
          " && aws s3 cp " + word + "/out.csv s3://synergist-prod-input/" + word + "/out.csv && " +
          "source deactivate");
      ProducerRecord<Long, String> record = new ProducerRecord<Long, String>("pipelines", 1L, word + " is done!");
      Producer<Long, String> producer = BeamKafkaProducer.createProducer();
      producer.send(record);
      producer.close();
      pb.inheritIO().start().waitFor();
      // Use ProcessContext.output to emit the output element.
      if (!word.isEmpty())
        c.output(word + "\n");
    }
  }
}
