package edu.lab;

import java.io.*;
import java.math.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Five lab tasks. All task-3 ordering is performed by Hadoop shuffle. */
public class CreditLab {
  static final String[] MONEY = {
    "CUST_SAVING_AMT",
    "CUST_FINA_AMT",
    "CUST_PRIFINA_AMT",
    "CUST_FOND_AMT",
    "CUST_NADEBT_AMT",
    "CUST_YBT_AMT",
    "CUST_METAL_AMT",
    "CUST_ASSET_AMT",
    "CUST_DEBT_AMT",
    "G_OS_PRCP_SUM"
  };
  static final String[] TYPES = {"存款", "理财", "私人理财", "基金", "国债", "银保通", "贵金属"};
  static final String[] TIERS = {"低资产", "中等资产", "较高资产", "高资产"};

  // Test lacks bad_good, so column positions are resolved by each file's header schema.
  static Map<String, Integer> schema(boolean train) {
    String[] a =
        ("CUST_ID,OPEN_ORG_NUM,"
                + (train ? "bad_good," : "")
                + String.join(",", MONEY)
                + ",LOAN_FLAG,CRED_FLAG")
            .split(",");
    Map<String, Integer> m = new HashMap<>();
    for (int i = 0; i < a.length; i++) m.put(a[i], i);
    return m;
  }

  static String cell(String[] r, Map<String, Integer> s, String k) {
    return r[s.get(k)].trim();
  }

  static BigDecimal money(String x) {
    return x.isEmpty() || x.equalsIgnoreCase("nan") || x.equalsIgnoreCase("null")
        ? BigDecimal.ZERO
        : new BigDecimal(x);
  }

  static long cents(String x) {
    return money(x).movePointRight(2).longValueExact();
  }

  static String amount(long c) {
    return BigDecimal.valueOf(c, 2).toPlainString();
  }

  static int tier(long a, long[] q) {
    return a <= q[0] ? 0 : a <= q[1] ? 1 : a <= q[2] ? 2 : 3;
  }

  static List<String> cached(Mapper.Context c) throws IOException {
    List<String> lines = new ArrayList<>();
    URI[] uris = c.getCacheFiles();
    if (uris == null) return lines;
    for (URI u : uris) {
      File local = new File(u.getFragment());
      InputStream in;
      if (local.isFile()) in = new FileInputStream(local);
      else {
        Path p = new Path(u.getScheme(), u.getAuthority(), u.getPath());
        in = p.getFileSystem(c.getConfiguration()).open(p);
      }
      try (BufferedReader b =
          new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String l;
        while ((l = b.readLine()) != null) lines.add(l);
      }
    }
    return lines;
  }

  public static class Base extends Mapper<LongWritable, Text, Text, Text> {
    Map<String, Integer> s;

    protected void setup(Context c) throws IOException {
      s = schema(c.getConfiguration().getBoolean("lab.train", true));
    }

    String[] row(Text t, Context c) throws IOException {
      String l = t.toString().replace("\ufeff", "");
      if (l.startsWith("CUST_ID,")) return null;
      String[] r = l.split(",", -1);
      if (r.length != s.size())
        throw new IOException("Invalid CSV row: expected " + s.size() + " columns");
      c.getCounter("LAB", "ROWS").increment(1);
      return r;
    }
  }

  public static class OrgMap extends Base {
    public void map(LongWritable k, Text t, Context c) throws IOException, InterruptedException {
      String[] r = row(t, c);
      if (r == null) return;
      c.write(
          new Text(cell(r, s, "OPEN_ORG_NUM")),
          new Text(
              "1,"
                  + cents(cell(r, s, "CUST_ASSET_AMT"))
                  + ","
                  + cents(cell(r, s, "CUST_DEBT_AMT"))
                  + ","
                  + cell(r, s, "bad_good")));
    }
  }

  public static class OrgSum extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text k, Iterable<Text> v, Context c)
        throws IOException, InterruptedException {
      long[] a = new long[4];
      for (Text t : v) {
        String[] z = t.toString().split(",");
        for (int i = 0; i < 4; i++) a[i] = Math.addExact(a[i], Long.parseLong(z[i]));
      }
      c.write(k, new Text(a[0] + "," + a[1] + "," + a[2] + "," + a[3]));
    }
  }

  public static class OrgReduce extends OrgSum {
    public void reduce(Text k, Iterable<Text> v, Context c)
        throws IOException, InterruptedException {
      long[] a = new long[4];
      for (Text t : v) {
        String[] z = t.toString().split(",");
        for (int i = 0; i < 4; i++) a[i] = Math.addExact(a[i], Long.parseLong(z[i]));
      }
      c.write(k, new Text(a[0] + "," + amount(a[1]) + "," + amount(a[2]) + "," + a[3]));
    }
  }

  public static class MeanMap extends Base {
    Map<String, BigDecimal[]> means = new HashMap<>();

    protected void setup(Context c) throws IOException {
      super.setup(c);
      for (String l : cached(c)) {
        String[] z = l.split("\t"), a = z[1].split(",");
        means.put(z[0], new BigDecimal[] {new BigDecimal(a[0]), new BigDecimal(a[1])});
      }
    }

    public void map(LongWritable k, Text t, Context c) throws IOException, InterruptedException {
      String[] r = row(t, c);
      if (r == null) return;
      String org = cell(r, s, "OPEN_ORG_NUM");
      BigDecimal[] m = means.get(org);
      boolean high = money(cell(r, s, "CUST_ASSET_AMT")).multiply(m[0]).compareTo(m[1]) >= 0;
      c.write(new Text(org), new Text(high ? "1,0" : "0,1"));
    }
  }

  public static class PairSum extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text k, Iterable<Text> v, Context c)
        throws IOException, InterruptedException {
      long a = 0, b = 0;
      for (Text t : v) {
        String[] z = t.toString().split(",");
        a += Long.parseLong(z[0]);
        b += Long.parseLong(z[1]);
      }
      c.write(k, new Text(a + "," + b));
    }
  }

  public static class QuantMap extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    public void map(LongWritable k, Text t, Context c) throws IOException, InterruptedException {
      String l = t.toString().replace("\ufeff", "");
      if (l.startsWith("CUST_ID,")) return;
      String[] r = l.split(",", -1);
      c.write(new LongWritable(cents(r[10])), new LongWritable(1));
    }
  }

  public static class CountSum
      extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    public void reduce(LongWritable k, Iterable<LongWritable> v, Context c)
        throws IOException, InterruptedException {
      long n = 0;
      for (LongWritable a : v) n += a.get();
      c.write(k, new LongWritable(n));
    }
  }

  public static class QuantReduce extends Reducer<LongWritable, LongWritable, Text, NullWritable> {
    long n, pos = 0;
    long[] q = new long[3];
    boolean[] found = new boolean[3];

    protected void setup(Context c) {
      n = c.getConfiguration().getLong("lab.n", 0);
      if (n == 0) throw new IllegalArgumentException("Empty training set");
    }

    public void reduce(LongWritable k, Iterable<LongWritable> v, Context c) {
      long count = 0;
      for (LongWritable a : v) count += a.get();
      for (int i = 0; i < 3; i++) {
        long rank = (n * (i + 1) + 3) / 4;
        if (!found[i] && pos + count >= rank) {
          q[i] = k.get();
          found[i] = true;
        }
      }
      pos += count;
    }

    protected void cleanup(Context c) throws IOException, InterruptedException {
      if (pos != n) throw new IOException("Row count mismatch");
      c.write(
          new Text("Q25:" + amount(q[0]) + "\tQ50:" + amount(q[1]) + "\tQ75:" + amount(q[2])),
          NullWritable.get());
    }
  }

  static long[] quantiles(Configuration c) {
    return new long[] {c.getLong("lab.q0", 0), c.getLong("lab.q1", 0), c.getLong("lab.q2", 0)};
  }

  public static class TierMap extends Base {
    long[] q;

    protected void setup(Context c) throws IOException {
      super.setup(c);
      q = quantiles(c.getConfiguration());
    }

    public void map(LongWritable k, Text t, Context c) throws IOException, InterruptedException {
      String[] r = row(t, c);
      if (r == null) return;
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < 7; i++) {
        if (i > 0) b.append(',');
        b.append(cents(cell(r, s, MONEY[i])));
      }
      c.write(new Text("" + tier(cents(cell(r, s, "CUST_ASSET_AMT")), q)), new Text(b.toString()));
    }
  }

  public static class TierSum extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text k, Iterable<Text> v, Context c)
        throws IOException, InterruptedException {
      long[] a = new long[7];
      for (Text t : v) {
        String[] z = t.toString().split(",");
        for (int i = 0; i < 7; i++) a[i] = Math.addExact(a[i], Long.parseLong(z[i]));
      }
      StringJoiner b = new StringJoiner(",");
      for (long x : a) b.add("" + x);
      c.write(k, new Text(b.toString()));
    }
  }

  public static class TierReduce extends Reducer<Text, Text, Text, NullWritable> {
    public void reduce(Text k, Iterable<Text> v, Context c)
        throws IOException, InterruptedException {
      long[] a = new long[7];
      for (Text t : v) {
        String[] z = t.toString().split(",");
        for (int i = 0; i < 7; i++) a[i] = Math.addExact(a[i], Long.parseLong(z[i]));
      }
      long total = 0;
      for (long x : a) total += x;
      Integer[] ix = {0, 1, 2, 3, 4, 5, 6};
      Arrays.sort(ix, (i, j) -> Long.compare(a[j], a[i]));
      for (int i : ix)
        c.write(
            new Text(
                TIERS[Integer.parseInt(k.toString())]
                    + "\t"
                    + TYPES[i]
                    + "\t"
                    + (total == 0
                        ? "0.00000000"
                        : BigDecimal.valueOf(a[i])
                            .divide(BigDecimal.valueOf(total), 8, RoundingMode.HALF_UP)
                            .toPlainString())),
            NullWritable.get());
    }
  }

  static String[] features(String[] r, Map<String, Integer> s, long[] q) {
    String[] f = new String[14];
    for (int i = 0; i < 10; i++) {
      double x = money(cell(r, s, MONEY[i])).doubleValue();
      f[i] =
          x < 0
              ? "NEG"
              : x == 0 ? "ZERO" : "B" + Math.min(30, (int) Math.floor(Math.log10(1 + x) * 2));
    }
    f[10] = cell(r, s, "OPEN_ORG_NUM");
    f[11] = cell(r, s, "LOAN_FLAG");
    f[12] = cell(r, s, "CRED_FLAG");
    f[13] = "" + tier(cents(cell(r, s, "CUST_ASSET_AMT")), q);
    return f;
  }

  public static class TrainMap extends Base {
    long[] q;

    protected void setup(Context c) throws IOException {
      super.setup(c);
      q = quantiles(c.getConfiguration());
    }

    public void map(LongWritable k, Text t, Context c) throws IOException, InterruptedException {
      String[] r = row(t, c);
      if (r == null) return;
      String y = cell(r, s, "bad_good");
      if (!y.equals("0") && !y.equals("1")) throw new IOException("Nonbinary label");
      c.write(new Text(y + "|N"), new Text("1"));
      String[] f = features(r, s, q);
      for (int i = 0; i < f.length; i++) c.write(new Text(y + "|" + i + "|" + f[i]), new Text("1"));
    }
  }

  public static class ModelSum extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text k, Iterable<Text> v, Context c)
        throws IOException, InterruptedException {
      long n = 0;
      for (Text t : v) n += Long.parseLong(t.toString());
      c.write(k, new Text("" + n));
    }
  }

  public static class PredictMap extends Base {
    long[] q;
    long[] n = new long[2];
    Map<String, Long> model = new HashMap<>();
    Map<Integer, Set<String>> vocab = new HashMap<>();

    protected void setup(Context c) throws IOException {
      super.setup(c);
      q = quantiles(c.getConfiguration());
      for (String l : cached(c)) {
        String[] a = l.split("\t"), key = a[0].split("\\|");
        long count = Long.parseLong(a[1]);
        model.put(a[0], count);
        if (key[1].equals("N")) n[Integer.parseInt(key[0])] = count;
        else vocab.computeIfAbsent(Integer.parseInt(key[1]), z -> new HashSet<>()).add(key[2]);
      }
      if (n[0] == 0 || n[1] == 0) throw new IOException("Both classes required");
    }

    public void map(LongWritable k, Text t, Context c) throws IOException, InterruptedException {
      String[] r = row(t, c);
      if (r == null) return;
      String[] f = features(r, s, q);
      double[] score = new double[2];
      for (int y = 0; y < 2; y++) {
        score[y] = Math.log((n[y] + 1.0) / (n[0] + n[1] + 2.0));
        for (int i = 0; i < f.length; i++)
          score[y] +=
              Math.log(
                  (model.getOrDefault(y + "|" + i + "|" + f[i], 0L) + 1.0)
                      / (n[y] + vocab.get(i).size() + 1.0));
      }
      double p = 1 / (1 + Math.exp(Math.max(-700, Math.min(700, score[0] - score[1]))));
      boolean detail = c.getConfiguration().getBoolean("lab.detail", false);
      c.write(
          new Text(cell(r, s, "CUST_ID")),
          new Text((p >= 0.5 ? "1" : "0") + (detail ? "\t" + p : "")));
    }
  }

  static Job job(
      Configuration c,
      String name,
      Path in,
      Path out,
      Class mapper,
      Class reducer,
      Class combiner,
      Class key,
      Class value,
      int reducers)
      throws Exception {
    Job j = Job.getInstance(c, name);
    j.setJarByClass(CreditLab.class);
    j.setMapperClass(mapper);
    j.setMapOutputKeyClass(key);
    j.setMapOutputValueClass(value);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(Text.class);
    if (reducer != null) j.setReducerClass(reducer);
    if (combiner != null) j.setCombinerClass(combiner);
    j.setNumReduceTasks(reducers);
    FileInputFormat.addInputPath(j, in);
    FileOutputFormat.setOutputPath(j, out);
    return j;
  }

  static void cache(Job j, Path dir) throws IOException {
    FileSystem fs = dir.getFileSystem(j.getConfiguration());
    int i = 0;
    for (FileStatus s : fs.listStatus(dir))
      if (s.getPath().getName().startsWith("part-"))
        j.addCacheFile(URI.create(s.getPath().toUri() + "#cache" + (i++)));
  }

  static List<String> read(Configuration c, Path dir) throws IOException {
    List<String> a = new ArrayList<>();
    FileSystem fs = dir.getFileSystem(c);
    for (FileStatus s : fs.listStatus(dir))
      if (s.getPath().getName().startsWith("part-"))
        try (BufferedReader b =
            new BufferedReader(
                new InputStreamReader(fs.open(s.getPath()), StandardCharsets.UTF_8))) {
          String l;
          while ((l = b.readLine()) != null) a.add(l);
        }
    return a;
  }

  static void run(Job j) throws Exception {
    long t = System.currentTimeMillis();
    if (!j.waitForCompletion(true)) throw new IOException("Failed " + j.getJobName());
    System.out.println(
        "LAB_SUCCESS\t"
            + j.getJobName()
            + "\t"
            + j.getJobID()
            + "\t"
            + (System.currentTimeMillis() - t)
            + " ms\tmap="
            + j.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue()
            + "\treduce="
            + j.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue());
  }

  public static void main(String[] args) throws Exception {
    Configuration c = new Configuration();
    List<String> a = new ArrayList<>();
    for (String x : args) {
      if (x.startsWith("-D")) {
        int p = x.indexOf('=');
        c.set(x.substring(2, p), x.substring(p + 1));
      } else a.add(x);
    }
    if (a.size() < 4)
      throw new IllegalArgumentException(
          "all train.csv test.csv output | train train.csv unused output | predict test.csv model"
              + " output [q0 q1 q2]");
    String mode = a.get(0);
    Path train = new Path(a.get(1)), test = new Path(a.get(2)), out = new Path(a.get(3));
    if (a.size() >= 7) for (int i = 0; i < 3; i++) c.setLong("lab.q" + i, cents(a.get(4 + i)));
    if (mode.equals("predict")) {
      c.setBoolean("lab.train", false);
      Job j =
          job(
              c,
              "task5-predict",
              train,
              out,
              PredictMap.class,
              null,
              null,
              Text.class,
              Text.class,
              0);
      cache(j, test);
      run(j);
      return;
    }
    if (mode.equals("all")) {
      Job j =
          job(
              c,
              "task1-org",
              train,
              new Path(out, "task1"),
              OrgMap.class,
              OrgReduce.class,
              OrgSum.class,
              Text.class,
              Text.class,
              1);
      run(j);
      j =
          job(
              c,
              "task2-mean",
              train,
              new Path(out, "task2"),
              MeanMap.class,
              PairSum.class,
              PairSum.class,
              Text.class,
              Text.class,
              1);
      cache(j, new Path(out, "task1"));
      run(j);
      long n = 0;
      for (String l : read(c, new Path(out, "task1")))
        n += Long.parseLong(l.split("\t")[1].split(",")[0]);
      c.setLong("lab.n", n);
      j =
          job(
              c,
              "task3-quantiles",
              train,
              new Path(out, "task3"),
              QuantMap.class,
              QuantReduce.class,
              CountSum.class,
              LongWritable.class,
              LongWritable.class,
              1);
      run(j);
      String[] qs = read(c, new Path(out, "task3")).get(0).split("\t");
      for (int i = 0; i < 3; i++) c.setLong("lab.q" + i, cents(qs[i].split(":")[1]));
      j =
          job(
              c,
              "task4-structure",
              train,
              new Path(out, "task4"),
              TierMap.class,
              TierReduce.class,
              TierSum.class,
              Text.class,
              Text.class,
              1);
      run(j);
    }
    Job j =
        job(
            c,
            "task5-train",
            train,
            new Path(out, "model"),
            TrainMap.class,
            ModelSum.class,
            ModelSum.class,
            Text.class,
            Text.class,
            1);
    run(j);
    if (mode.equals("all")) {
      c.setBoolean("lab.train", false);
      j =
          job(
              c,
              "task5-predict",
              test,
              new Path(out, "task5"),
              PredictMap.class,
              null,
              null,
              Text.class,
              Text.class,
              0);
      cache(j, new Path(out, "model"));
      run(j);
    }
  }
}
