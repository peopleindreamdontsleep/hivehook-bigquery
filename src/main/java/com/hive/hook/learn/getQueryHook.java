package com.hive.hook.learn;



import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import org.codehaus.jackson.map.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.hive.hook.learn.HookUtils.getListPathSize;
import static com.hive.hook.learn.HookUtils.getValueName;

public class getQueryHook implements ExecuteWithHookContext {

    private static final HashSet<String> OPERATION_NAMES = new HashSet<>();

    static {
        OPERATION_NAMES.add(HiveOperation.CREATETABLE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE_OWNER.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_ADDCOLS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_LOCATION.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_PROPERTIES.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAME.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAMECOL.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_REPLACECOLS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPTABLE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
    }

    @Override
    public void run(HookContext hookContext) throws Exception {
        HashMap<String, Object> hiveSqlParseValue = new HashMap<>();

        QueryPlan plan = hookContext.getQueryPlan();

        ObjectMapper mapper = new ObjectMapper();
        String operationName = plan.getOperationName();
        //System.out.println("Query executed: " + plan.getQueryString());
        hiveSqlParseValue.put("operation",operationName);//获取当前操作
        hiveSqlParseValue.put("user",hookContext.getUserName());

        Long timeStamp = System.currentTimeMillis();  //获取当前时间戳

        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sd = sdf.format(new Date(Long.parseLong(String.valueOf(timeStamp))));
        hiveSqlParseValue.put("time",sd);
        hiveSqlParseValue.put("sql",plan.getQueryString());//当前查询sql
        hiveSqlParseValue.put("hookType",hookContext.getHookType());
        hiveSqlParseValue.put("queryId",plan.getQueryId());
        ArrayList<String> tableList = new ArrayList<>();
        ArrayList<String> inputPutList = new ArrayList<>();
        ArrayList<String> inputPutPartitionList = new ArrayList<>();
        HashSet<String> ownerSet = new HashSet<>();
        int isPartition=0;
        if (OPERATION_NAMES.contains(operationName)
                && !plan.isExplain()) {
            Set<ReadEntity> inputs = hookContext.getInputs();
//            Set<WriteEntity> outputs = hookContext.getOutputs();
            for (Entity entity : inputs) {
                switch (entity.getType()) {

                    //无分区
                    case TABLE:
                        tableList.add(entity.getTable().getTTable().getTableName());
                        inputPutList.add(mapper.writeValueAsString(entity.getTable().getTTable().getSd().getFieldValue(StorageDescriptor._Fields.LOCATION)).replace("\"",""));
                        ownerSet.add(entity.getTable().getTTable().getOwner());
                    //有分区
                    case PARTITION:
                        if (null != entity.getP()){
                            isPartition=1;
                            String partitionName = getValueName(entity.getP().getSpec(), false);
                            String influxPath = mapper.writeValueAsString(entity.getTable().getTTable().getSd().getFieldValue(StorageDescriptor._Fields.LOCATION)).replace("\"","");
                            inputPutPartitionList.add(influxPath+"/"+partitionName);
                        }

                }
            }

//            for (Entity entity : outputs) {

//            }

        }

        hiveSqlParseValue.put("inputTableList",tableList);

        if (isPartition==0){
            hiveSqlParseValue.put("inputPaths",inputPutList);
            hiveSqlParseValue.put("totalSize",getListPathSize(inputPutList));
        }else{
            hiveSqlParseValue.put("inputPaths",inputPutPartitionList);
            hiveSqlParseValue.put("totalSize",getListPathSize(inputPutPartitionList));
        }


        hiveSqlParseValue.put("app.owner",ownerSet);
        String resultJson = mapper.writeValueAsString(hiveSqlParseValue);
        System.out.println(resultJson);
    }


}

