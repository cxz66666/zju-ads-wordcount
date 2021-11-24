
import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import java.io.IOException;
        import java.util.StringTokenizer;

        //本类无实际用处
//仅用来说明一个笔者的思考
//但是实际上并没有什么用

public class SortedWordWithoutDouble_NoUse {


    public static class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable> {


        private static final String REGEXP_CHARS_NUMS = "[^\\p{L}\\p{Nd}]+";

        private final  IntWritable one = new IntWritable(1);
        private Text word = new Text();
        //split each word and convert to name:1
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                word.set(st.nextToken().replaceAll(REGEXP_CHARS_NUMS, "").trim().toLowerCase());
                if(word.getLength()>0){
                    context.write(word, one);
                }
            }
        }
    }
    //convert to num:name
    public static class IntSumReducer  extends Reducer<Text,IntWritable,LongWritable,Text> {
        private LongWritable result = new LongWritable(0);

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(Long.valueOf(sum));
            //but you can't do this because combine and reduce must have same struct
            context.write( result,key);
        }
    }

    public static class RepeatReducer  extends Reducer<LongWritable,Text,Text,LongWritable> {

        private Text result = new Text();

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // a trivial loop
            // write to file(context is the destination)
            for (Text val : values) {
                result.set(val);
                context.write( result,key);
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path outputFolderPath = new Path(args[1]);
        outputFolderPath.getFileSystem(conf).delete(outputFolderPath, true);

        Job jobCount = Job.getInstance(conf, "word count");//init
        jobCount.setJarByClass(SortedWordWithoutDouble_NoUse.class);//main class
        jobCount.setMapperClass(TokenizerMapper.class);//map
        jobCount.setCombinerClass(IntSumReducer.class);//combine
        jobCount.setReducerClass(RepeatReducer.class);//reduce
        jobCount.setMapOutputKeyClass(Text.class);//key class
        jobCount.setMapOutputValueClass(IntWritable.class);//value class
        jobCount.setOutputKeyClass(Text.class);//output key class
        jobCount.setOutputValueClass(LongWritable.class);
        jobCount.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        FileInputFormat.addInputPath(jobCount, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobCount, outputFolderPath);
        boolean success = jobCount.waitForCompletion(true);
        //whether success
        System.exit(success ? 0 : 1);

    }
}