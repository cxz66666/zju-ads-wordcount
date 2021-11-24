
/*
 * 并行版 word count
 * 用法
 * 1. build it
 * 2.  jar cf wc.jar *.class
 * 3. 找到打包好的 time hadoop jar wc.jar  WordCount  /path/to/input   /path/to/output
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class WordCount {

    // map 函数，接受<行号，文本>，返回<单词,1>
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // combine 函数，将相同的单词的键值对<单词，1>聚合成<单词，N>
    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    // reduce 函数，包含reduce和cleanup两个阶段
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // 新建了一个跳表<频次，<字符串集合>>
        private static Map<Integer, ConcurrentSkipListSet<String>> skipList = new ConcurrentSkipListMap<Integer, ConcurrentSkipListSet<String>>(new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                return y.compareTo(x);
            }
        });

        // reduce 的结果放进跳表里
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 计算总频次
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 频次出现过的就在跳表中加新单词，否则就新建跳表集合
            if (skipList.containsKey(sum)){
                ConcurrentSkipListSet<String> value = skipList.get(sum);
                value.add(key.toString());
                skipList.put(sum,value);
            }else{
                ConcurrentSkipListSet<String> value = new ConcurrentSkipListSet<String>();
                value.add(key.toString());
                skipList.put(sum, value);
            }
        }

        // 输出结果
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 优先考虑频次
            for (Integer key : skipList.keySet()) {
                // 然后考虑字典序
                for (String word:skipList.get(key)){
                    context.write(new Text(word), new IntWritable(key));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        long startTime=System.currentTimeMillis();

        // if /path/to/output exist, then we delete it firstly
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        Job job = Job.getInstance(conf, "word count");

        //set the map, reduce class
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(IntSumReducer.class);

        //set key class
        //set value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // input path and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);

        long endTime=System.currentTimeMillis();

        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");

        System.exit(0);
    }
}
