package lob;

public class LocalCSVParser {

    /**
     * 优化版解析器：不创建 String[]，直接填充复用的 LobRecord 对象
     * ✅ 修复：全套安全解析 (Int, Long, Float, Double)，遇空值自动归零
     */
    public static boolean parse(String line, LobRecord r) {
        if (line == null || line.isEmpty()) return false;

        int start = 0;
        int end = line.indexOf(',');
        int colIndex = 0;

        while (end != -1) {
            parseColumn(line, start, end, colIndex, r);
            start = end + 1;
            end = line.indexOf(',', start);
            colIndex++;
        }
        parseColumn(line, start, line.length(), colIndex, r);

        return colIndex >= 56;
    }

    private static void parseColumn(String line, int start, int end, int colIndex, LobRecord r) {
        if (colIndex == 0) {
            // tradingDay 原逻辑是 parseDouble 转 int，为了兼容性这里保持原样
            // 如果你的 CSV 里肯定是整数 (如 20230101 而不是 20230101.0)，建议改用 parseIntSafe
            r.tradingDay = (int) parseDoubleSafe(line, start, end);
        } else if (colIndex == 1) {
            r.tradeTime = parseLongSafe(line, start, end);
        } else if (colIndex == 8) {
            r.last = parseDoubleSafe(line, start, end);
        } else if (colIndex == 12) {
            r.tBidVol = parseDoubleSafe(line, start, end);
        } else if (colIndex == 13) {
            r.tAskVol = parseDoubleSafe(line, start, end);
        } else if (colIndex >= 17 && colIndex <= 36) {
            int offset = colIndex - 17;
            int levelIdx = offset / 4;
            int type = offset % 4;

            double val = parseDoubleSafe(line, start, end);
            switch (type) {
                case 0: r.bp[levelIdx] = val; break;
                case 1: r.bv[levelIdx] = val; break;
                case 2: r.ap[levelIdx] = val; break;
                case 3: r.av[levelIdx] = val; break;
            }
        }
    }

    // ==========================================
    // ⭐ 全套安全解析工具箱 (含 Float)
    // ==========================================

    // Double 安全解析
    private static double parseDoubleSafe(String line, int start, int end) {
        if (start >= end) return 0.0;
        return Double.parseDouble(line.substring(start, end));
    }

    // Long 安全解析
    private static long parseLongSafe(String line, int start, int end) {
        if (start >= end) return 0L;
        return Long.parseLong(line.substring(start, end));
    }

    // Int 安全解析 (备用)
    private static int parseIntSafe(String line, int start, int end) {
        if (start >= end) return 0;
        return Integer.parseInt(line.substring(start, end));
    }

    // Float 安全解析
    private static float parseFloatSafe(String line, int start, int end) {
        if (start >= end) return 0.0f;
        return Float.parseFloat(line.substring(start, end));
    }
}