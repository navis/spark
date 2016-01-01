package org.apache.spark.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 */
public class SimpleCombiner<K, V> implements InputFormat<K, V> {

  private final InputFormat<K, V> delegate;
  private final long threshold;

  public SimpleCombiner(InputFormat<K, V> delegate, long threshold) {
    this.delegate = delegate;
    this.threshold = threshold;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] original = delegate.getSplits(job, numSplits);
    List<Pair<Set<String>, List<InputSplit>>> splits = new ArrayList<>();
    for (InputSplit split : original) {
      final long length = split.getLength();
      final String[] locations = split.getLocations();

      boolean added = false;
      for (Pair<Set<String>, List<InputSplit>> entry : splits) {
        if (entry.n > threshold) {
          continue;
        }
        Set<String> set = entry.l;
        if (containsAny(set, locations)) {
          set.retainAll(Arrays.asList(locations));
          entry.r.add(split);
          entry.n += length;
          added = true;
          break;
        }
      }
      if (!added) {
        splits.add(new Pair<Set<String>, List<InputSplit>>(
            length,
            new HashSet<>(Arrays.asList(locations)),
            new ArrayList<>(Arrays.asList(split))));
      }
    }

    List<InputSplit> combined = new ArrayList<>();
    Iterator<Pair<Set<String>, List<InputSplit>>> iterator = splits.iterator();
    while (iterator.hasNext()) {
      Pair<Set<String>, List<InputSplit>> entry = iterator.next();
      if (entry.n >= threshold) {
        combined.add(
            new InputSplits(entry.n,
                entry.l.toArray(new String[entry.l.size()]),
                entry.r.toArray(new InputSplit[entry.r.size()])));
        iterator.remove();
      }
    }


    Pair<Set<String>, List<InputSplit>> current = null;
    iterator = splits.iterator();
    while (iterator.hasNext()) {
      Pair<Set<String>, List<InputSplit>> entry = iterator.next();
      if (current == null) {
        iterator.remove();
        current = entry;
        continue;
      }
      if (containsAny(current.l, entry.l)) {
        iterator.remove();
        current.n += entry.n;
        current.r.addAll(entry.r);
        current.l.retainAll(entry.l);
      }
      if (current.n > threshold) {
        combined.add(
            new InputSplits(current.n,
                current.l.toArray(new String[current.l.size()]),
                current.r.toArray(new InputSplit[current.r.size()])));
        current = null;
      }
    }
    if (current != null) {
      combined.add(
          new InputSplits(current.n,
              current.l.toArray(new String[current.l.size()]),
              current.r.toArray(new InputSplit[current.r.size()])));
    }
    for (Pair<Set<String>, List<InputSplit>> entry : splits) {
      combined.add(new InputSplits(entry.n,
          entry.l.toArray(new String[entry.l.size()]),
          entry.r.toArray(new InputSplit[entry.r.size()])));
    }
    return combined.toArray(new InputSplit[combined.size()]);
  }

  private boolean containsAny(Set<String> set, String[] targets) {
    return containsAny(set, Arrays.asList(targets));
  }

  private boolean containsAny(Set<String> set, Collection<String> targets) {
    for (String target : targets) {
      if (set.contains(target)) {
        return true;
      }
    }
    return set.isEmpty();
  }

  @Override
  public RecordReader<K, V> getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
      throws IOException {

    final InputSplit[] splits = ((InputSplits) split).splits;

    return new RecordReader<K, V>() {

      int index;
      long pos;
      RecordReader<K, V> reader = nextReader();

      private RecordReader<K, V> nextReader() throws IOException {
        return delegate.getRecordReader(splits[index++], job, reporter);
      }

      @Override
      @SuppressWarnings("unchecked")
      public boolean next(K key, V value) throws IOException {
        while (!reader.next(key, value)) {
          if (index < splits.length) {
            pos = reader.getPos();
            reader.close();
            reader = nextReader();
            continue;
          }
          return false;
        }
        return true;
      }

      @Override
      public K createKey() {
        return reader.createKey();
      }

      @Override
      public V createValue() {
        return reader.createValue();
      }

      @Override
      public long getPos() throws IOException {
        return pos + reader.getPos();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return (index - 1 + reader.getProgress()) / splits.length;
      }
    };
  }

  private static class InputSplits implements InputSplit {

    private long length;
    private InputSplit[] splits;
    private String[] locations;

    InputSplits(long length, String[] locations, InputSplit[] splits) {
      this.length = length;
      this.locations = locations;
      this.splits = splits;
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public String[] getLocations() throws IOException {
      return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(length);
      out.writeInt(locations.length);
      for (String location : locations) {
        out.writeUTF(location);
      }
      out.writeInt(splits.length);
      for (InputSplit split : splits) {
        out.writeUTF(split.getClass().getName());
        split.write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      length = in.readLong();
      locations = new String[in.readInt()];
      for (int i = 0; i < locations.length; i++) {
        locations[i] = in.readUTF();
      }
      splits = new InputSplit[in.readInt()];
      try {
        for (int i = 0; i < splits.length; i++) {
          splits[i] = (InputSplit) Class.forName(in.readUTF()).newInstance();
          splits[i].readFields(in);
        }
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String toString() {
      return "length = " + length + ", locations = " + Arrays.toString(locations) + ", splits = " + Arrays.toString(splits);
    }
  }

  private static class Pair<L, R> {
    private long n;
    private final L l;
    private final R r;

    private Pair(long n, L l, R r) {
      this.n = n;
      this.l = l;
      this.r = r;
    }

    static <L, R> Pair<L, R> of(long n, L l, R r) {
      return new Pair<>(n, l, r);
    }
  }

  private static List<FileStatus> getFiles(FileSystem fs, Path p) throws IOException {
    List<FileStatus> collect = new ArrayList<>();
    getFiles(fs, fs.getFileStatus(p), collect);
    return collect;
  }

  private static void getFiles(FileSystem fs, FileStatus p, List<FileStatus> collect) throws IOException {
    if (p.isDirectory()) {
      for (FileStatus c : fs.listStatus(p.getPath())) {
        getFiles(fs, c, collect);
      }
    } else {
      collect.add(p);
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("org.apache.hadoop.hive.common.io.SimpleCombiner " +
          "<input-format-class> <input-directory> [-t <threshold>] [-p <num-partition>]");
      return;
    }
    JobConf conf = new JobConf();
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    InputFormat inputFormat = (InputFormat) Class.forName(args[0]).newInstance();
    if (inputFormat instanceof Configurable) {
      ((Configurable) inputFormat).setConf(conf);
    } else if (inputFormat instanceof JobConfigurable) {
      ((JobConfigurable) inputFormat).configure(conf);
    }

    FileSystem fs = FileSystem.get(conf);

    StringBuilder builder = new StringBuilder();
    for (FileStatus f : getFiles(fs, new Path(args[1]))) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      builder.append(f.getPath());
    }

    conf.set(FileInputFormat.INPUT_DIR, builder.toString());

    long threshold = fs.getDefaultBlockSize();
    int numPartitions = 16;
    for (int i = 2; i < args.length; i++) {
      String trim = args[i].trim();
      if (trim.equals("-t")) {
        threshold = Long.valueOf(args[++i]);
      } else if (trim.equals("-p")) {
        numPartitions = Integer.valueOf(args[++i]);
      }
    }

    SimpleCombiner combiner = new SimpleCombiner(inputFormat, threshold);

    InputSplit[] original = inputFormat.getSplits(conf, numPartitions);
    System.out.println("original " + original.length + " partitions");
    for (InputSplit split : original) {
      System.out.println("-- split = " + split);
    }
    System.out.println();
    InputSplit[] splits = combiner.getSplits(conf, numPartitions);
    System.out.println("combined " + original.length + " partitions");
    for (InputSplit split : splits) {
      System.out.println("-- split = " + split);
    }

    System.in.read();

    for (InputSplit split : splits) {
      System.out.println(split);
      RecordReader reader = combiner.getRecordReader(split, conf, Reporter.NULL);
      Object key = reader.createKey();
      Object value = reader.createValue();
      while (reader.next(key, value)) {
        System.out.println(reader.getPos() + ":" + reader.getProgress() + " --> " + key + " = " + value);
      }
    }
  }
}
