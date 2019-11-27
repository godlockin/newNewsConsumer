package com.exception;

import com.common.constants.ResultEnum;
import lombok.Data;

@Data
public class ConsumerException extends Exception {

    private Integer code;

    public ConsumerException(ResultEnum result) {
        super(result.getMessage());
        this.code = result.getCode();
    }

    public ConsumerException(ResultEnum result, String message) {
        super(message);
        this.code = result.getCode();
    }

    public ConsumerException(Integer code, String msg) {
        super(msg);
        this.code = code;
    }
}
