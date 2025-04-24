

from common.logger import LoggerFactory
from database.initialise_database import TableFactory


TABLE_ORDER = [
    "users",
    "drivers",
    "car_components",
    "reading_end_point",
    "events"
]


# This script creates the tables in the database and populates them with data from CSV files. It also ensures that foreign key constraints are added after the data has been inserted. 
# The script uses the pandas library to read data from CSV files and the mysql.connector library to interact with the MySQL database.
if __name__ == "__main__":
    logger = LoggerFactory.get_logger(__name__)
    table_factory = TableFactory()
    table_factory.create_tables()
    logger.info("🎉 Database successfully populated!")
