package lob;

public class LocalCSVParser {

    // 专门给 LocalTest 和 Mapper 共用的解析函数
    public static LobRecord parse(String line) {

        String[] arr = line.split(",");

        // snapshot.csv 最少 57 列
        if (arr.length < 57) {
            System.err.println("Invalid line (len=" + arr.length + "): " + line);
            return null;
        }

        LobRecord r = new LobRecord();

        // —— 基本字段 ——
        r.tradingDay = (int) Double.parseDouble(arr[0]);
        r.tradeTime  = Long.parseLong(arr[1]);   // HHMMSS，不带毫秒（你 LocalTest 已确认）

        // —— 价格类字段需要缩放（除以 10000） ——
        r.last = Double.parseDouble(arr[8]) / 10000.0;

        // —— 成交量相关字段（不缩放） ——
        r.tBidVol = Double.parseDouble(arr[12]);
        r.tAskVol = Double.parseDouble(arr[13]);

        // —— 5 档行情（从 10 档中取前 5 档） ——
        int levelStart = 17;

        for (int level = 0; level < 5; level++) {
            int base = levelStart + level * 4;

            r.bp[level] = Double.parseDouble(arr[base])     / 10000.0;
            r.bv[level] = Double.parseDouble(arr[base + 1]);
            r.ap[level] = Double.parseDouble(arr[base + 2]) / 10000.0;
            r.av[level] = Double.parseDouble(arr[base + 3]);
        }

        return r;
    }
}
