//表明这个类属于edu.homework包
package edu.homework;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Two chained MapReduce jobs: set membership, then per-index counts. */
//Configured 继承Hadoop的配置管理能力
//implements Tool 遵守Hadoop Tool接口要求，提供run方法
public class IndexSetRelations extends Configured implements Tool {
    //保存“指数编号+股票代码”
    public static class StockKey implements WritableComparable<StockKey> {
        private int index;  //指数编号
        private final Text stock = new Text();  //final表示stock这一变量不能再指向另外一个text对象
        public StockKey() { }
        public void set(int i, String s) { index = i; stock.set(s); }
        public void write(DataOutput out) throws IOException { out.writeInt(index); stock.write(out); }
        public void readFields(DataInput in) throws IOException { index = in.readInt(); stock.readFields(in); }
        public int compareTo(StockKey other) {
            int c = Integer.compare(index, other.index);
            return c == 0 ? stock.compareTo(other.stock) : c;
        }
        //键的hash值，决定进入哪个reducer。这里只有一个reducer，所有键进入同一分区
        public int hashCode() { return 31 * index + stock.hashCode(); }
        public boolean equals(Object o) {
            return o instanceof StockKey && compareTo((StockKey) o) == 0;
        }
        public String toString() { return index + "\t" + stock; }
    }

    //将一行输入转换为键值对
    //<输入键，输入值，输出键，输出值>
    public abstract static class SourceMapper extends Mapper<LongWritable, Text, StockKey, IntWritable> {
        private final StockKey keyOut = new StockKey();
        protected abstract int sourceMask();
        //三个参数：当前行的字节偏移量；当前行的文字；与hadoop交互的上下文
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            //按连续空白拆成字符串数组
     /*        fields[0] = "101"
            fields[1] = "JPM"   */ 
            String[] fields = line.split("\\s+");
            if (fields.length != 2) throw new IOException("Expected index and stock: " + line);
            try { keyOut.set(Integer.parseInt(fields[0]), fields[1]); }
            catch (NumberFormatException ex) { throw new IOException("Invalid index: " + line, ex); }
            ctx.write(keyOut, new IntWritable(sourceMask()));
        }
    }
    public static class AMapper extends SourceMapper { protected int sourceMask() { return 1; } }
    public static class BMapper extends SourceMapper { protected int sourceMask() { return 2; } }

    // OR is associative, commutative and idempotent: repeated rows count only once.
    public static class MaskCombiner extends Reducer<StockKey, IntWritable, StockKey, IntWritable> {
        protected void reduce(StockKey key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int mask = 0;
            for (IntWritable value : values) mask |= value.get();
            ctx.write(key, new IntWritable(mask));
        }
    }
    public static class RelationReducer extends Reducer<StockKey, IntWritable, StockKey, Text> {
        protected void reduce(StockKey key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int mask = 0;
            for (IntWritable value : values) mask |= value.get();
            String relation = mask == 3 ? "BOTH" : mask == 1 ? "A_ONLY" : "B_ONLY";
            ctx.write(key, new Text(relation));
            ctx.getCounter("Relations", relation).increment(1);
        }
    }

    public static class Counts implements Writable {
        private long both, aOnly, bOnly;
        public Counts() { }
        public Counts(long b, long a, long c) { both = b; aOnly = a; bOnly = c; }
        public void write(DataOutput out) throws IOException {
            out.writeLong(both); out.writeLong(aOnly); out.writeLong(bOnly);
        }
        public void readFields(DataInput in) throws IOException {
            both = in.readLong(); aOnly = in.readLong(); bOnly = in.readLong();
        }
        public void add(Counts c) { both += c.both; aOnly += c.aOnly; bOnly += c.bOnly; }
        public String toString() { return both + "\t" + aOnly + "\t" + bOnly + "\t" + (both + aOnly + bOnly); }
    }
    public static class CountMapper extends Mapper<LongWritable, Text, IntWritable, Counts> {
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] fields = line.split("\\s+");
            if (fields.length != 3) throw new IOException("Invalid task1 row: " + line);
            Counts count;
            switch (fields[2]) {
                case "BOTH": count = new Counts(1, 0, 0); break;
                case "A_ONLY": count = new Counts(0, 1, 0); break;
                case "B_ONLY": count = new Counts(0, 0, 1); break;
                default: throw new IOException("Unknown relation: " + line);
            }
            try { ctx.write(new IntWritable(Integer.parseInt(fields[0])), count); }
            catch (NumberFormatException ex) { throw new IOException("Invalid index: " + line, ex); }
        }
    }
    public static class CountCombiner extends Reducer<IntWritable, Counts, IntWritable, Counts> {
        protected void reduce(IntWritable key, Iterable<Counts> values, Context ctx)
                throws IOException, InterruptedException {
            Counts total = new Counts();
            for (Counts value : values) total.add(value);
            ctx.write(key, total);
        }
    }
    public static class CountReducer extends Reducer<IntWritable, Counts, IntWritable, Text> {
        protected void reduce(IntWritable key, Iterable<Counts> values, Context ctx)
                throws IOException, InterruptedException {
            Counts total = new Counts();
            for (Counts value : values) total.add(value);
            ctx.write(key, new Text(total.toString()));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: IndexSetRelations <A> <B> <output_task1> <output_task2>");
            return 2;
        }
        Path firstOutput = new Path(args[2]), secondOutput = new Path(args[3]);
        if (firstOutput.equals(secondOutput)) throw new IOException("Output paths must differ");
        // Check both directories before submitting either job; never delete existing data.
        if (firstOutput.getFileSystem(getConf()).exists(firstOutput)
                || secondOutput.getFileSystem(getConf()).exists(secondOutput)) {
            throw new IOException("Output directory already exists; use new paths");
        }
        //创建作业对象并设置名称
        Job first = Job.getInstance(getConf(), "homework5-task1-set-relations");
        first.setJarByClass(IndexSetRelations.class);
        //分别设置两个输入路径
        MultipleInputs.addInputPath(first, new Path(args[0]), TextInputFormat.class, AMapper.class);
        MultipleInputs.addInputPath(first, new Path(args[1]), TextInputFormat.class, BMapper.class);
        //设置中间类型
        first.setMapOutputKeyClass(StockKey.class);
        first.setMapOutputValueClass(IntWritable.class);
        //设置计算组件
        first.setCombinerClass(MaskCombiner.class);
        first.setReducerClass(RelationReducer.class);
        //设置最终输出类型
        first.setOutputKeyClass(StockKey.class);
        first.setOutputValueClass(Text.class);
        //只使用一个reduce任务
        first.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(first, firstOutput);
        if (!first.waitForCompletion(true)) return 1;

        Job second = Job.getInstance(getConf(), "homework5-task2-set-counts");
        second.setJarByClass(IndexSetRelations.class);
        second.setMapperClass(CountMapper.class);
        second.setMapOutputKeyClass(IntWritable.class);
        second.setMapOutputValueClass(Counts.class);
        second.setCombinerClass(CountCombiner.class);
        second.setReducerClass(CountReducer.class);
        second.setOutputKeyClass(IntWritable.class);
        second.setOutputValueClass(Text.class);
        second.setNumReduceTasks(1);
        FileInputFormat.addInputPath(second, firstOutput);
        FileOutputFormat.setOutputPath(second, secondOutput);
        return second.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new IndexSetRelations(), args));
    }
}
