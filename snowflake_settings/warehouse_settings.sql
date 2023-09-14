ALTER WAREHOUSE COMPUTE_WH
SET AUTO_SUSPEND = 60; -- Set the inactivity period in seconds, e.g., 300 seconds (5 minutes)

ALTER WAREHOUSE COMPUTE_WH
SET AUTO_RESUME = TRUE;