import logging

# Database configuration
DATABASE_PATH = "db/db.duckdb"

# Logging configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# Scheduling parameters
HOURS_BLOCKS = 24

# Constraint flags
ENABLE_WORK_INDICATOR = True
ENABLE_TRANSITION = True
ENABLE_CONSECUTIVE_DAYS_OFF = True
ENABLE_HOURS = True
ENABLE_WORKLOAD = True 