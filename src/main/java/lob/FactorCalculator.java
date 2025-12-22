package lob;

public class FactorCalculator {

    //防止除以零的微小常数
    private static final double EPS = 1e-7;
    //盘口档位数量 (5档行情)
    private static final int N = 5;

    /**
     * 计算所有因子并将结果存入预分配的buffer数组中
     * 目的：通过传入buffer避免在每次计算时创建新的 double[]数组，减轻垃圾回收压力
     *
     * @param prev 上一笔切片的行情快照 (用于计算时序相关的因子，如价格变化)
     * @param cur  当前这一笔行情快照
     * @param f    用于存放计算结果的数组 (长度为20)
     */
    public static void computeAll(LobRecord prev, LobRecord cur, double[] f) {
        //提取当前快照的基础数据，减少后续重复访问对象的开销
        double ap1 = cur.ap[0]; // 卖一价
        double bp1 = cur.bp[0]; // 买一价
        double av1 = cur.av[0]; // 卖一量
        double bv1 = cur.bv[0]; // 买一量

        //计算中间价
        double mid = 0.5 * (ap1 + bp1);

        //利用LobRecord中的辅助方法计算累积量
        double bidDepth = cur.sumBidVol(N);      // 买单总深度
        double askDepth = cur.sumAskVol(N);      // 卖单总深度
        double bidPrcVol = cur.sumBidPrcVol(N);  // 买单金额 (价*量)
        double askPrcVol = cur.sumAskPrcVol(N);  // 卖单金额 (价*量)

        //因子计算

        //f[0]: 最优价差
        f[0] = ap1 - bp1;
        //f[1]: 相对价差
        f[1] = (ap1 - bp1) / (mid + EPS);
        //f[2]: 中间价
        f[2] = mid;

        //f[3]: 买一不平衡
        f[3] = (bv1 - av1) / (bv1 + av1 + EPS);
        // f[4]: 多档不平衡
        f[4] = (bidDepth - askDepth) / (bidDepth + askDepth + EPS);

        // f[5] - f[8]: 买卖方深度、深度差、深度比
        f[5] = bidDepth;
        f[6] = askDepth;
        f[7] = bidDepth - askDepth;
        f[8] = bidDepth / (askDepth + EPS);

        // f[9]: 买卖量平衡指数
        double tb = cur.tBidVol;
        double ta = cur.tAskVol;
        f[9] = (tb - ta) / (tb + ta + EPS);

        // f[10] - f[11]: 买卖方加权价格
        double vwapBid = bidPrcVol / (bidDepth + EPS);
        double vwapAsk = askPrcVol / (askDepth + EPS);
        f[10] = vwapBid;
        f[11] = vwapAsk;

        // f[12]: 加权中间价
        f[12] = (bidPrcVol + askPrcVol) / (bidDepth + askDepth + EPS);
        // f[13]: 加权价差
        f[13] = vwapAsk - vwapBid;

        // f[14]: 买卖密度差
        f[14] = (bidDepth - askDepth) / N;

        // f[15]: 买卖不对称度
        double wBid = 0.0, wAsk = 0.0;
        for (int i = 0; i < N; i++) {
            int lvl = i + 1;
            //权重为 1/Level (1, 1/2, 1/3...)
            wBid += cur.bv[i]/(double) lvl;
            wAsk += cur.av[i]/(double) lvl;
        }
        f[15] = (wBid - wAsk) / (wBid + wAsk + EPS);

        //时序相关因子 (需要用到 prev)
        if (prev == null) {
            //如果没有前一笔数据，变化量设为 0
            f[16] = 0.0;
            f[17] = 0.0;
            f[18] = 0.0;
        } else {
            // f[16]: 最优价变动
            f[16] = cur.ap[0]-prev.ap[0];

            // f[17]: 中间价变化
            double midPrev = 0.5 * (prev.ap[0] + prev.bp[0]);
            f[17] = mid - midPrev;

            // f[18]: 深度比变动
            double prevBD = prev.sumBidVol(N);
            double prevAD = prev.sumAskVol(N);
            double ratio = bidDepth/(askDepth + EPS);
            double ratioPrev = prevBD/(prevAD + EPS);
            f[18] = ratio - ratioPrev;
        }

        // f[19]:价压指标
        f[19] = (ap1- bp1) / (bidDepth + askDepth + EPS);
    }
}