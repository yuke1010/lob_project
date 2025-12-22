package lob;

public class LobRecord {

    //交易日 (yyyyMMdd)
    public int tradingDay;
    //交易时间 (HHmmssSSS)
    public long tradeTime;

    //申买价 (Bid Price) 5档
    public double[] bp = new double[5];
    //申买量 (Bid Volume) 5档
    public double[] bv = new double[5];
    //申卖价 (Ask Price) 5档
    public double[] ap = new double[5];
    //申卖量 (Ask Volume) 5档
    public double[] av = new double[5];

    //总买/卖成交量 (累计值)
    public double tBidVol;
    public double tAskVol;
    //最新成交价
    public double last;

    // 辅助计算方法，简化 FactorCalculator的代码

    // 计算前n档的买单总量
    public double sumBidVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += bv[i];
        return s;
    }

    // 计算前n档的卖单总量
    public double sumAskVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += av[i];
        return s;
    }

    // 计算前n档买单的金额加权量(Price * Volume)
    public double sumBidPrcVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += bp[i] * bv[i];
        return s;
    }

    // 计算前n档卖单的金额加权量
    public double sumAskPrcVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += ap[i] * av[i];
        return s;
    }
}