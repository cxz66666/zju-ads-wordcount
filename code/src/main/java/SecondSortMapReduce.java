import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
    对应两次map reduce
    时间会非常长

 */



public class SecondSortMapReduce {
    public static  class CombinationKey implements WritableComparable<CombinationKey>{

        private IntWritable firstKey;
        private Text secondKey;

        //无参构造函数
        public CombinationKey() {
            this.firstKey = new IntWritable();
            this.secondKey = new Text();
        }

        //有参构造函数
        public CombinationKey(IntWritable firstKey, Text secondKey) {
            this.firstKey = firstKey;
            this.secondKey = secondKey;
        }

        public IntWritable getFirstKey() {
            return firstKey;
        }

        public void setFirstKey(IntWritable firstKey) {
            this.firstKey = firstKey;
        }

        public Text getSecondKey() {
            return secondKey;
        }

        public void setSecondKey(Text secondKey) {
            this.secondKey = secondKey;
        }

        public void write(DataOutput out) throws IOException {
            this.firstKey.write(out);
            this.secondKey.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            this.firstKey.readFields(in);
            this.secondKey.readFields(in);
        }


	/*public int compareTo(CombinationKey combinationKey) {
		int minus = this.getFirstKey().compareTo(combinationKey.getFirstKey());
		if (minus != 0){
			return minus;
		}
		return this.getSecondKey().get() - combinationKey.getSecondKey().get();
	}*/
        /**
         * 自定义比较策略
         * 注意：该比较策略用于MapReduce的第一次默认排序
         * 也就是发生在Map端的sort阶段
         * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
         */
        public int compareTo(CombinationKey combinationKey) {
            return this.firstKey.compareTo(combinationKey.getFirstKey());
        }

//    @Override
//    public int hashCode() {
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + ((firstKey == null) ? 0 : firstKey.hashCode());
//        return result;
//    }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CombinationKey other = (CombinationKey) obj;
            if (firstKey == null) {
                if (other.firstKey != null)
                    return false;
            } else if (!firstKey.equals(other.firstKey))
                return false;
            return true;
        }


    }




    public static class DefinedGroupSort extends WritableComparator {
        protected DefinedGroupSort() {
            super(CombinationKey.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CombinationKey combinationKey1 = (CombinationKey) a;
            CombinationKey combinationKey2 = (CombinationKey) b;

            //自定义按原始数据中第一个key分组
            return combinationKey1.getFirstKey().compareTo(combinationKey2.getFirstKey());
        }
    }

    public static class DefinedPartition extends Partitioner<CombinationKey, IntWritable> {

        /**
         * 数据输入来源：map输出 我们这里根据组合键的第一个值作为分区
         * 如果不自定义分区的话，MapReduce会根据默认的Hash分区方法
         * 将整个组合键相等的分到一个分区中，这样的话显然不是我们要的效果
         *
         * @param key           map输出键值
         * @param value         map输出value值
         * @param numPartitions 分区总数，即reduce task个数
         */
        public int getPartition(CombinationKey key, IntWritable value, int numPartitions) {
            return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static class DefinedComparator extends WritableComparator {

        protected DefinedComparator() {
            super(CombinationKey.class,true);
        }
        /**
         * 第一列按降序排列，第二列也按升序排列
         */
        public int compare(WritableComparable a, WritableComparable b) {
            CombinationKey c1 = (CombinationKey) a;
            CombinationKey c2 = (CombinationKey) b;
            int minus = c2.getFirstKey().compareTo(c1.getFirstKey());

            if (minus != 0){
                return minus;
            } else {
                return c1.getSecondKey().compareTo(c2.getSecondKey());
            }
        }
    }
    ///////////////////////////////
    /////////////////////////////////
    // map 函数，接受<行号，文本>，返回<单词,1>
    public static class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable> {


        private static final String REGEXP_CHARS_NUMS = "[^\\p{L}\\p{Nd}]+";

        private final  IntWritable one = new IntWritable(1);
        private final Text word = new Text();

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
    //combine 和  reduce 函数，将相同的单词的键值对<单词，1>聚合成<单词，N>
    public static class IntSumReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final  IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    //    // map 函数，接受<行号，文本>，返回<<1,string>,string>
    public static class SecondMapper extends Mapper<Object, Text, CombinationKey, Text>{

        private final  IntWritable number = new IntWritable(0);
        private final Text word = new Text();
        private final CombinationKey combinationKey = new CombinationKey();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                number.set(Integer.parseInt(st.nextToken()));
                combinationKey.setFirstKey(number);
                combinationKey.setSecondKey(word);
                context.write(combinationKey,word);
            }
        }
    }
    //sort and write back
    public static class RepeatReducer  extends Reducer<CombinationKey,Text,Text,IntWritable> {

        private final Text result = new Text();
        private  IntWritable totalNumber=new IntWritable();
        public void reduce(CombinationKey key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (Text val : values) {
                result.set(val);
                totalNumber=key.getFirstKey();
                context.write( result,totalNumber);
            }

        }
    }
    private static final String OUTPUT_FOLDER_COUNT = "swc_output_count";


    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        String OUTPUT_FOLDER_SORT=args[1];
        Configuration conf = new Configuration();
        Path outputFolderPath = new Path(OUTPUT_FOLDER_COUNT);
        outputFolderPath.getFileSystem(conf).delete(outputFolderPath, true);

        Path outputSortFolderPath = new Path(OUTPUT_FOLDER_SORT);
        outputSortFolderPath.getFileSystem(conf).delete(outputSortFolderPath, true);


        Job jobCount = Job.getInstance(conf, "word count");
        jobCount.setJarByClass(SecondSortMapReduce.class);
        jobCount.setMapperClass(TokenizerMapper.class);
        jobCount.setCombinerClass(IntSumReducer.class);
        jobCount.setReducerClass(IntSumReducer.class);
        jobCount.setOutputKeyClass(Text.class);
        jobCount.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(jobCount, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobCount, outputFolderPath);
        boolean success = jobCount.waitForCompletion(true);

        if (success) {
            Job jobOrder = Job.getInstance(conf, "word count");
            conf.set("mapred.map.tasks","5");
            jobOrder.setJarByClass(SecondSortMapReduce.class);
            jobOrder.setMapperClass(SecondMapper.class);
            jobOrder.setMapOutputKeyClass(CombinationKey.class);
            jobOrder.setMapOutputValueClass(Text.class);
//            jobOrder.setCombinerClass(RepeatReducer.class);

            jobOrder.setPartitionerClass(DefinedPartition.class);

            //设置自定义比较策略(因为我的CombineKey重写了compareTo方法，所以这个可以省略)
            jobOrder.setSortComparatorClass(DefinedComparator.class);
            //设置自定义分组策略
            jobOrder.setGroupingComparatorClass(DefinedGroupSort.class);

            jobOrder.setReducerClass(RepeatReducer.class);
            jobOrder.setOutputKeyClass(Text.class);
            jobOrder.setOutputValueClass(IntWritable.class);



            FileInputFormat.addInputPath(jobOrder, new Path(OUTPUT_FOLDER_COUNT+System.getProperty("file.separator")+"part-r-00000"));
            FileOutputFormat.setOutputPath(jobOrder, new Path(OUTPUT_FOLDER_SORT));
            success = jobOrder.waitForCompletion(true);

        }
        outputFolderPath = new Path(OUTPUT_FOLDER_COUNT);
        outputFolderPath.getFileSystem(conf).delete(outputFolderPath, true);
        long endTime=System.currentTimeMillis();
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
        System.exit(success ? 0 : 1);

    }
}
