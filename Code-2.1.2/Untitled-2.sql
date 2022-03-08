CREATE OR REPLACE PROCEDURE "BATCH_WEEKLY_INIT"("BATCH_ID" FLOAT)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

    cr_stmt=snowflake.createStatement({sqlText : "INSERT INTO BATCH_RUN_LOG(BATCH_ID,BATCH_RUN_STATUS) VALUES (?,?);",
    binds:[BATCH_ID,"Started"]
    }
    );
     cr_stmt.execute();
     
     return "Batch Run Id for Weekly batch generated successfully";
        
  ';

  SnowflakeOperator(
                task_id='eds_stored_procedure1',
                # Change the snowflake conn_id
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                warehouse='DEV_WH_DAAS_ETL',
                # warehouse='WH_DAAS_DEVELOPER',
                # Change to appropriate snowflake role
                role='DAAS_ETL_DEV_ROLE',
                database='DEV_DAAS',
                # Change to appropriate schema
                schema='EDS',
                # Change to the stored procedure name
                sql=' CALL SRD_DAAS_EDS_FREEWHEEL_CAMPAIGN_TVDATA_DETAIL(333);'
                
            ) 


            CREATE OR REPLACE PROCEDURE "PROCESS_INIT"("PROCESS_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
   sql_stmt1= `SELECT PROCESS_ID FROM LOG.PROCESS_CONFIG WHERE PROCESS_NAME=?`;
   sql_stmt2= `SELECT MAX(BATCH_RUN_ID) AS BATCH_RUN_ID FROM LOG.BATCH_RUN_LOG WHERE BATCH_ID IN(SELECT BATCH_ID FROM PROCESS_CONFIG WHERE PROCESS_NAME=?) and BATCH_RUN_STATUS = ''Started''
     `;
   cr_stmt1=snowflake.createStatement({sqlText : sql_stmt1, binds:[PROCESS_NAME]});
   cr_stmt2=snowflake.createStatement({sqlText : sql_stmt2, binds:[PROCESS_NAME]});
   
   var res1=cr_stmt1.execute();
   res1.next();
   processid=res1.getColumnValue(1);
   
   var res2=cr_stmt2.execute();
   res2.next();
   batch_runid=res2.getColumnValue(1);
   
     
    cr_stmt3=snowflake.createStatement({sqlText : "INSERT INTO PROCESS_RUN_LOG(PROCESS_ID,BATCH_RUN_ID,PROCESS_RUN_STATUS) VALUES (?,?,?);",
    binds:[processid,batch_runid,"Started"]
    }
    );
     cr_stmt3.execute();
     
     return "Process Run Id generated successfully";
     ';