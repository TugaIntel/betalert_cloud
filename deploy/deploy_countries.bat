@echo off
SET FUNCTION_NAME=countries
SET TRIGGER_TYPE=--trigger-http
SET RUNTIME=python39
SET ENTRY_POINT=main_countries
SET REGION=europe-west1
SET SOURCE_PATH=D:\TugaIntel\BetAlert_Cloud\scripts

REM Deploy the function
gcloud functions deploy %FUNCTION_NAME% --runtime %RUNTIME% %TRIGGER_TYPE% --entry-point %ENTRY_POINT% --source %SOURCE_PATH% --region %REGION%
