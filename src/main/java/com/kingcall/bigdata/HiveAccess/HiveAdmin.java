package com.kingcall.bigdata.HiveAccess;

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * 自定义Hive的超级用户
 *
 * @author 01
 * @date 2020-11-09
 **/
public class HiveAdmin extends AbstractSemanticAnalyzerHook {

    /**
     * 定义超级用户，可以定义多个
     */
    private static final String[] ADMINS = {"root"};

    /**
     * 权限类型列表
     */
    private static final int[] TOKEN_TYPES = {
            HiveParser.TOK_CREATEDATABASE, HiveParser.TOK_DROPDATABASE,
            HiveParser.TOK_CREATEROLE, HiveParser.TOK_DROPROLE,
            HiveParser.TOK_GRANT, HiveParser.TOK_REVOKE,
            HiveParser.TOK_GRANT_ROLE, HiveParser.TOK_REVOKE_ROLE,
            HiveParser.TOK_CREATETABLE
    };

    /**
     * 获取当前登录的用户名
     *
     * @return 用户名
     */
    private String getUserName() {
        boolean hasUserName = SessionState.get() != null &&
                SessionState.get().getAuthenticator().getUserName() != null;

        return hasUserName ? SessionState.get().getAuthenticator().getUserName() : null;
    }

    private boolean isInTokenTypes(int type) {
        for (int tokenType : TOKEN_TYPES) {
            if (tokenType == type) {
                return true;
            }
        }

        return false;
    }

    private boolean isAdmin(String userName) {
        for (String admin : ADMINS) {
            if (admin.equalsIgnoreCase(userName)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
        if (!isInTokenTypes(ast.getToken().getType())) {
            return ast;
        }

        String userName = getUserName();
        if (isAdmin(userName)) {
            return ast;
        }

        throw new SemanticException(userName +
                " is not Admin, except " +
                Joiner.on(",").join(ADMINS)
        );
    }
}
