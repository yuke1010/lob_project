package lob;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class LocalTest {

    private static final String SNAPSHOT_PATH = "/home/hadoop/data/snapshot.csv";

    public static void main(String[] args) throws Exception {

        List<LobRecord> list = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(SNAPSHOT_PATH), StandardCharsets.UTF_8))) {

            String line = br.readLine(); // skip header
            System.out.println("Header = " + line);

            while ((line = br.readLine()) != null) {
                LobRecord r = parseLine(line);
                if (r == null) continue;
                if (r.tradeTime < 92500) continue; // ignore before 092500
                list.add(r);
            }
        }

        Collections.sort(list, Comparator.comparingLong(o -> o.tradeTime));

        LobRecord prev = null;

        for (LobRecord cur : list) {

            // only print result for time >= 093000
            if (cur.tradeTime >= 93000) {
                double[] f = FactorCalculator.computeAll(prev, cur);
                printFactors(cur.tradeTime, f);
            }

            prev = cur;
        }
        int count = 0;

        for (LobRecord cur : list) {

            if (cur.tradeTime >= 93000) {
                double[] f = FactorCalculator.computeAll(prev, cur);
                printFactors(cur.tradeTime, f);

                if (++count >= 20) break; // 输出前20行以后停止
            }

            prev = cur;
        }

    }

    private static LobRecord parseLine(String line) {

        String[] arr = line.split(",");

        if (arr.length < 57) {
            System.err.println("Invalid line (len=" + arr.length + "): " + line);
            return null;
        }

        LobRecord r = new LobRecord();

        r.tradingDay = Integer.parseInt(arr[0]);
        r.tradeTime  = Long.parseLong(arr[1]);

        // 价格类字段 = 原始值 / 10000
        r.last = Double.parseDouble(arr[8]) / 10000.0;

        r.tBidVol = Double.parseDouble(arr[12]);
        r.tAskVol = Double.parseDouble(arr[13]);

        int levelStart = 17;

        // 10 档行情——我们只取前 5 档
        for (int level = 0; level < 5; level++) {
            int base = levelStart + level * 4;
            r.bp[level] = Double.parseDouble(arr[base])     / 10000.0;
            r.bv[level] = Double.parseDouble(arr[base + 1]);              // 量不需要缩放
            r.ap[level] = Double.parseDouble(arr[base + 2]) / 10000.0;
            r.av[level] = Double.parseDouble(arr[base + 3]);              // 量不需要缩放
        }

        return r;
    }


    private static void printFactors(long time, double[] f) {
        System.out.printf("%06d  ", time);
        for (int i = 0; i < f.length; i++) {
            System.out.printf("f%-2d=%.6f  ", i + 1, f[i]);
        }
        System.out.println();
    }
}
