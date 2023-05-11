package slidewindows.wtp.debedium;

import lombok.Getter;

public enum DebeziumOptionTypeEnum {
    //新增create
    C("c"),
    //更新update
    U("u"),
    ///删除delete
    D("d"),
    //读read
    R("r"),

    ;
    private final String op;


    DebeziumOptionTypeEnum(String op) {
        this.op = op;
    }

    public String getOp() {
        return op;
    }
}
