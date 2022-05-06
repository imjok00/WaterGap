package org.min.watergap.common.position.incre;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * MYSQL UUID:seque 格式
 *
 * @Create by metaX.h on 2022/4/30 11:24
 */
public class UUIDSet {

    public UUID SID;
    public List<Interval> intervals;

    public static UUIDSet parse(String str) {
        String[] ss = str.split(":");

        if (ss.length < 2) {
            throw new RuntimeException(String.format("parseUUIDSet failed due to wrong format: %s", str));
        }

        List<Interval> intervals = new ArrayList<>();
        for (int i = 1; i < ss.length; i++) {
            intervals.add(parseInterval(ss[i]));
        }

        UUIDSet uuidSet = new UUIDSet();
        uuidSet.SID = UUID.fromString(ss[0]);
        uuidSet.intervals = combine(intervals);

        return uuidSet;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(SID.toString());
        for (Interval interval : intervals) {
            if (interval.start == interval.stop - 1) {
                sb.append(":");
                sb.append(interval.start);
            } else {
                sb.append(":");
                sb.append(interval.start);
                sb.append("-");
                sb.append(interval.stop - 1);
            }
        }

        return sb.toString();
    }

    /**
     * 解析如下格式字符串为Interval: 1 => Interval{start:1, stop:2} 1-3 =>
     * Interval{start:1, stop:4} 注意！字符串格式表达时[n,m]是两侧都包含的，Interval表达时[n,m)右侧开
     *
     * @param str
     * @return
     */
    public static Interval parseInterval(String str) {
        String[] ss = str.split("-");

        Interval interval = new Interval();
        switch (ss.length) {
            case 1:
                interval.start = Long.parseLong(ss[0]);
                interval.stop = interval.start + 1;
                break;
            case 2:
                interval.start = Long.parseLong(ss[0]);
                interval.stop = Long.parseLong(ss[1]) + 1;
                break;
            default:
                throw new RuntimeException(String.format("parseInterval failed due to wrong format: %s", str));
        }

        return interval;
    }

    /**
     * 把{start,stop}连续的合并掉: [{start:1, stop:4},{start:4, stop:5}] => [{start:1,
     * stop:5}]
     *
     * @param intervals
     * @return
     */
    public static List<Interval> combine(List<Interval> intervals) {
        List<Interval> combined = new ArrayList<>();
        Collections.sort(intervals);
        int len = intervals.size();
        for (int i = 0; i < len; i++) {
            combined.add(intervals.get(i));

            int j;
            for (j = i + 1; j < len; j++) {
                if (intervals.get(i).stop >= intervals.get(j).start) {
                    if (intervals.get(i).stop < intervals.get(j).stop) {
                        intervals.get(i).stop = intervals.get(j).stop;
                    }
                } else {
                    break;
                }
            }
            i = j - 1;
        }

        return combined;
    }

    public static class Interval implements Comparable<Interval> {

        public long start;
        public long stop;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Interval interval = (Interval) o;

            if (start != interval.start) return false;
            return stop == interval.stop;
        }

        @Override
        public int hashCode() {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (stop ^ (stop >>> 32));
            return result;
        }

        @Override
        public int compareTo(Interval o) {
            if (equals(o)) {
                return 1;
            }
            return Long.compare(start, o.start);
        }
    }
}
