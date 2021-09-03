package com.hive.hook.learn;


import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;


public class hookTest implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) {
        System.out.println("the first pre hook");
    }
}
