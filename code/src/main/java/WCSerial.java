/*
 * 串行版 word count
 * 用法
 * 1. 安装maven
 * 2. 运行 mvn package
 * 3. 找到打包好的jar java -jar ***.jar  /path/to/input   /path/to/output
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.StringTokenizer;

public class WCSerial {

    private static class CountForWord implements Comparable<CountForWord> {
        String word;
        int count = 1;

        public CountForWord(String word) {
            this.word = word;
        }
        //override the count function
        @Override
        public int compareTo(CountForWord t) {
            if (count < t.count) {
                return 1;
            } else if (count > t.count) {
                return -1;
            } else {
                return word.compareTo(t.word);
            }
        }
    }

    private static void tokenizeAndSubmit(Map<String, CountForWord> m, String line) {
        String trimmed = line.trim();
        if (!line.isEmpty()) {
            StringTokenizer tok = new StringTokenizer(line, " ");
            while (tok.hasMoreTokens()) {
                String word = tok.nextToken();
                CountForWord c = m.get(word);
                if (c != null) {
                    c.count++;
                } else {
                    m.put(word, new CountForWord(word));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // File inputPath = new File("../test/");
        // File[] inputs = inputPath.listFiles();
        long startTime=System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        FSDataInputStream fsDataInputStream = fs.open(new Path(args[0]));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(args[1]));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
        BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));


        Map<String, CountForWord> m = new HashMap<String, CountForWord>();
        String line;
        while ((line = br.readLine()) != null) {
            //get the whole line and send it to tokenize
            tokenizeAndSubmit(m, line);
        }
        br.close();
        fsDataInputStream.close();
        ArrayList<CountForWord> lst = new ArrayList<>(m.values());
        //sort it
        Collections.sort(lst);
        //write to the output file destination
        for (CountForWord c : lst) {
            bw.write(c.word + "\t" + c.count + "\n");
        }
        //flush and close
        bw.flush();
        bw.close();
        //also flush and close
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        fs.close();
        long endTime=System.currentTimeMillis();
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
        //always exit 0
        System.exit(  0);

    }
}