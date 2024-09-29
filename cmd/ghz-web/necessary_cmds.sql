--  ALTER TABLE details
--  ADD COLUMN subscription_id TEXT;
 
--  DELETE from details
--  WHERE subscription_id == "set_call"

-- SELECT "databroker_enter_timestamp", "databroker_exit_timestamp", "latency", "request_process_ts", "client_to_broker_ts", "broker_to_client_ts", "subscription_id" 
-- from details where "subscription_id"=="set_call"
-- 
-- SELECT "databroker_exit_timestamp", "broker_to_client_ts", "subscription_id" 
-- from details where "subscription_id"!="set_call"
-- 
-- select * from details where "request_id"=="1"



select request_id, request_process_ts, databroker_enter_timestamp, databroker_exit_timestamp, broker_to_client_ts, set_id from details where "request_id"=="50sub10pub" and subscription_id != "set_call" order by set_id asc 

-- delete from details where "request_id"=="10sub1000pub"

DELETE FROM details
WHERE rowid IN (
    SELECT rowid
    FROM details WHERE "request_id"=="10sub1000pub"
    ORDER BY created_at DESC
    LIMIT 510
);