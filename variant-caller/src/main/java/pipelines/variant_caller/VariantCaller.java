package pipelines.variant_caller;

import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
//import org.apache.beam.sdk.coders.KvCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import pipelines.variant_caller.AddLines;
//import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;


public class VariantCaller
{
    public static void main( String[] args )
    {
        PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(opts);
        PCollection<String> lines = p.apply(TextIO.read().from("test_in.csv"));
        PCollection<String> mergedLines = lines.apply(Combine.globally(new AddLines()));
        mergedLines.apply(TextIO.write().to("test_out.csv"));
        p.run();
    }
}


