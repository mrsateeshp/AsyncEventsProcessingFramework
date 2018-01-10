package com.thoughtstream.aepf;

import java.nio.charset.Charset;

/**
 * @author Sateesh Pinnamaneni
 * @since 28/12/2017
 */
public interface DefaultConstants {
    Charset DEFAULT_CHAR_SET = Charset.forName("UTF-8");
    static void exec(ExecHandler ignoreException){
        try {
            ignoreException.run();
        } catch (Exception e) {
            //ignore
        }
    }

    interface ExecHandler{
        void run() throws Exception;
    }
}
