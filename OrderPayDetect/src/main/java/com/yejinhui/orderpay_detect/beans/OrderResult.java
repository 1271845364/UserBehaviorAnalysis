package com.yejinhui.orderpay_detect.beans;

/**
 * @Date 2021/1/23 16:29
 * @Created by huijinye@126.com
 */
public class OrderResult {

    private Long orderId;

    private String resultState;

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", resultState='" + resultState + '\'' +
                '}';
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getResultState() {
        return resultState;
    }

    public void setResultState(String resultState) {
        this.resultState = resultState;
    }

    public OrderResult(Long orderId, String resultState) {
        this.orderId = orderId;
        this.resultState = resultState;
    }

    public OrderResult() {
    }
}
