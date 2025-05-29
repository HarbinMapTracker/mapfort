package com.mapfort.mapfortservice.common;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "通用响应包装")
public class Result<T> {

    private static final String SUCCESS = "200";
    private static final String ERROR = "-1";

    @Schema(description = "状态码", example = "200")
    private String code;

    @Schema(description = "消息", example = "操作成功")
    private String msg;

    @Schema(description = "返回数据")
    private T data;

    public static <T> Result<T> success() {
        Result<T> result = new Result<>();
        result.setCode(SUCCESS);
        result.setMsg("成功");
        return result;
    }

    public static <T> Result<T> success(T data) {
        Result<T> result = new Result<>();
        result.setCode(SUCCESS);
        result.setMsg("成功");
        result.setData(data);
        return result;
    }

    public static <T> Result<T> error(String msg) {
        Result<T> result = new Result<>();
        result.setCode(ERROR);
        result.setMsg(msg);
        return result;
    }

    // Getters and Setters

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
