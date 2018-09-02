package pipelines.variant_caller;

import java.util.List;
import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
//import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
//import org.apache.beam.sdk.coders.KvCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;

import pipelines.variant_caller.AddLines;
import pipelines.variant_caller.LaunchDocker;
//import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;


public class VariantCaller
{
    public static void main( String[] args )
    {
        PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(opts);
        // Create a Java Collection, in this case a List of Strings.
        final List<String> LINES = Arrays.asList(
          "sample1,params1",
          "sample2,params2",
          "sample3,params3");
        //PCollection<String> lines = p.apply(TextIO.read().from("hdfs://test_in.csv"));
        // Apply Create, passing the list and the coder, to create the PCollection.
        PCollection<String> lines = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
        PCollection<String> outLines = lines.apply(ParDo.of(new LaunchDocker.LaunchJobs()));
        PCollection<String> mergedLines = outLines.apply(Combine.globally(new AddLines()));
        //mergedLines.apply(TextIO.write().to("hdfs://test_out.csv"));
        p.run();
    }
}


