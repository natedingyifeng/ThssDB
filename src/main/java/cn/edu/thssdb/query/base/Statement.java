package cn.edu.thssdb.query.base;

import cn.edu.thssdb.utils.Global;

public class Statement {
    public Global.STMT_TYPE type;
    public Object[] params;

    public Statement(Global.STMT_TYPE type, Object[] params){
        this.type = type;
        this.params = params;
    }
}
