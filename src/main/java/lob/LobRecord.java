package lob;

public class LobRecord {

    public int tradingDay;
    public long tradeTime;

    public double[] bp = new double[5];
    public double[] bv = new double[5];
    public double[] ap = new double[5];
    public double[] av = new double[5];

    public double tBidVol;
    public double tAskVol;
    public double last;

    public double sumBidVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += bv[i];
        return s;
    }

    public double sumAskVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += av[i];
        return s;
    }

    public double sumBidPrcVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += bp[i] * bv[i];
        return s;
    }

    public double sumAskPrcVol(int n) {
        double s = 0.0;
        for (int i = 0; i < n; i++) s += ap[i] * av[i];
        return s;
    }
}
