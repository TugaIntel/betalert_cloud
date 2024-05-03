@echo off
SET PROJECT_ID=innate-empire-422116-u4
SET FUNCTION_NAME=teams
SET TRIGGER_TYPE=--trigger-http
SET RUNTIME=python312
SET ENTRY_POINT=teams_main
SET REGION=europe-west1
SET TIMEOUT=540
SET SOURCE_PATH=D:\TugaIntel\BetAlert_Cloud\functions
SET ENV_VARS=INSTANCE_CONNECTION_NAME=innate-empire-422116-u4:europe-west1:betalert,DB_USER=betadmin,PROJECT_ID=innate-empire-422116-u4,DB_NAME=BetAlert,PRIVATE_IP=false

REM Deploy the function using gcloud
gcloud functions deploy %FUNCTION_NAME% --project %PROJECT_ID% --runtime %RUNTIME% %TRIGGER_TYPE% --entry-point=%ENTRY_POINT% --source=%SOURCE_PATH%\%FUNCTION_NAME% --region=%REGION% --no-gen2 --set-env-vars=%ENV_VARS% --timeout=%TIMEOUT%


