package lob;

public class LocalCSVParser {

    /**
     * 优化版解析器：手动遍历字符串查找逗号
     * 优势：不产生 String[]数组，不产生substring垃圾对象（除非需要转数字）
     * 直接将解析结果填充到复用的 LobRecord对象中
     */
    public static boolean parse(String line, LobRecord r) {
        if (line == null || line.isEmpty()) return false;

        int start = 0;
        int end = line.indexOf(',');
        int colIndex = 0;

        //循环查找逗号，分割字段
        while (end != -1) {
            parseColumn(line, start, end, colIndex, r);
            start = end + 1;
            end = line.indexOf(',', start); //查找下一个逗号
            colIndex++;
        }
        //处理最后一个字段（没有逗号结尾）
        parseColumn(line, start, line.length(), colIndex, r);

        //简单校验列数，确保数据完整性
        return colIndex >= 56;
    }

    //根据列索引将数据解析并赋值给LobRecord的对应字段
    private static void parseColumn(String line, int start, int end, int colIndex, LobRecord r) {
        if (colIndex == 0) {
            //解析交易日，兼容Double格式的字符串
            r.tradingDay = (int) parseDoubleSafe(line, start, end);
        } else if (colIndex == 1) {
            //解析时间戳
            r.tradeTime = parseLongSafe(line, start, end);
        } else if (colIndex == 8) {
            //解析最新价
            r.last = parseDoubleSafe(line, start, end);
        } else if (colIndex == 12) {
            r.tBidVol = parseDoubleSafe(line, start, end);
        } else if (colIndex == 13) {
            r.tAskVol = parseDoubleSafe(line, start, end);
        } else if (colIndex >= 17 && colIndex <= 36) {
            //解析5档盘口数据
            //原始数据是展平的，需要计算偏移量映射到数组索引
            int offset = colIndex - 17; //数据是从第 17 列开始的
            int levelIdx = offset / 4; //计算是第几档 (0-4),每4个数据是一组,代表同一档位，比如买一价、买一量、卖一价、卖一量）
            int type = offset % 4;     //计算是哪种数据 (BP, BV, AP, AV)

            double val = parseDoubleSafe(line, start, end);
            switch (type) {
                case 0: r.bp[levelIdx] = val; break; //Bid Price买价
                case 1: r.bv[levelIdx] = val; break; //Bid Volume买量
                case 2: r.ap[levelIdx] = val; break; //Ask Price卖价
                case 3: r.av[levelIdx] = val; break; //Ask Volume卖量
            }
        }
    }


    //安全解析工具箱：处理空字符串和异常情况
    //安全解析 Double，如果字段为空或越界返回 0.0
    private static double parseDoubleSafe(String line, int start, int end) {
        if (start >= end) return 0.0;
        return Double.parseDouble(line.substring(start, end));
    }

    //安全解析 Long
    private static long parseLongSafe(String line, int start, int end) {
        if (start >= end) return 0L;
        return Long.parseLong(line.substring(start, end));
    }

    //Int安全解析
    private static int parseIntSafe(String line, int start, int end) {
        if (start >= end) return 0;
        return Integer.parseInt(line.substring(start, end));
    }

    //Float安全解析
    private static float parseFloatSafe(String line, int start, int end) {
        if (start >= end) return 0.0f;
        return Float.parseFloat(line.substring(start, end));
    }
}