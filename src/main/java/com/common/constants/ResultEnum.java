package com.common.constants;

public enum ResultEnum implements BaseEnum {

    SUCCESS(0, "成功")
    ,FAILURE(1, "失败")
    ,PARAMETER_CHECK(11, "参数校验失败")

    ,ES_CLIENT_INIT(120, "ES 初始化失败")
    ,ES_CLIENT_CLOSE(121, "ES 连接关闭")
    ;

    private Integer code;
    private String message;

    ResultEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public Integer getCode() {
        return this.code;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
