package com.yejinhui.market_analysis.beans;

/**
 * @Date 2021/1/21 14:22
 * @Created by huijinye@126.com
 */
public class AdCountViewByProvince {

    private String province;

    private String windowEnd;

    private Long count;

    @Override
    public String toString() {
        return "AdCountViewByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public AdCountViewByProvince(String province, String windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public AdCountViewByProvince() {
    }
}
