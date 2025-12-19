package lob;

public class LocalCSVParser {

    public static LobRecord parse(String line) {

        String[] arr = line.split(",");
        if (arr.length < 57) return null;

        LobRecord r = new LobRecord();

        r.tradingDay = (int) Double.parseDouble(arr[0]);
        r.tradeTime = Long.parseLong(arr[1]);

        // 整数价格，不缩放（后面统一 *10000）
        r.last = Double.parseDouble(arr[8]);

        r.tBidVol = Double.parseDouble(arr[12]);
        r.tAskVol = Double.parseDouble(arr[13]);

        int base = 17;
        for (int i = 0; i < 5; i++) {
            int idx = base + i * 4;
            r.bp[i] = Double.parseDouble(arr[idx]);
            r.bv[i] = Double.parseDouble(arr[idx + 1]);
            r.ap[i] = Double.parseDouble(arr[idx + 2]);
            r.av[i] = Double.parseDouble(arr[idx + 3]);
        }

        return r;
    }
}
