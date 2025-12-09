package lob;

public class FactorCalculator {

    private static final double EPS = 1e-7;
    private static final int N = 5;

    public static double[] computeAll(LobRecord prev, LobRecord cur) {
        double[] f = new double[20];

        double ap1 = cur.ap[0];
        double bp1 = cur.bp[0];
        double av1 = cur.av[0];
        double bv1 = cur.bv[0];

        double mid = 0.5 * (ap1 + bp1);
        double bidDepth = cur.sumBidVol(N);
        double askDepth = cur.sumAskVol(N);
        double bidPrcVol = cur.sumBidPrcVol(N);
        double askPrcVol = cur.sumAskPrcVol(N);

        f[0] = ap1 - bp1;
        f[1] = (ap1 - bp1) / (mid + EPS);
        f[2] = mid;

        f[3] = (bv1 - av1) / (bv1 + av1 + EPS);
        f[4] = (bidDepth - askDepth) / (bidDepth + askDepth + EPS);

        f[5] = bidDepth;
        f[6] = askDepth;
        f[7] = bidDepth - askDepth;
        f[8] = bidDepth / (askDepth + EPS);

        double tb = cur.tBidVol;
        double ta = cur.tAskVol;
        f[9] = (tb - ta) / (tb + ta + EPS);

        double vwapBid = bidPrcVol / (bidDepth + EPS);
        double vwapAsk = askPrcVol / (askDepth + EPS);
        f[10] = vwapBid;
        f[11] = vwapAsk;

        f[12] = (bidPrcVol + askPrcVol) / (bidDepth + askDepth + EPS);
        f[13] = vwapAsk - vwapBid;

        f[14] = (bidDepth - askDepth) / N;

        double wBid = 0.0, wAsk = 0.0;
        for (int i = 0; i < N; i++) {
            int lvl = i + 1;
            wBid += cur.bv[i] / (double) lvl;
            wAsk += cur.av[i] / (double) lvl;
        }
        f[15] = (wBid - wAsk) / (wBid + wAsk + EPS);

        if (prev == null) {
            f[16] = 0.0;
            f[17] = 0.0;
            f[18] = 0.0;
        } else {
            f[16] = cur.ap[0] - prev.ap[0];

            double midPrev = 0.5 * (prev.ap[0] + prev.bp[0]);
            f[17] = mid - midPrev;

            double prevBD = prev.sumBidVol(N);
            double prevAD = prev.sumAskVol(N);
            double ratio = bidDepth / (askDepth + EPS);
            double ratioPrev = prevBD / (prevAD + EPS);
            f[18] = ratio - ratioPrev;
        }

        f[19] = (ap1 - bp1) / (bidDepth + askDepth + EPS);

        return f;
    }
}
