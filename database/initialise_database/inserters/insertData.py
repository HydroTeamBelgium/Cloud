from common.logger import LoggerFactory
from database.initialise_database.inserters.InsertFactory import InsertFactory

def main():
    logger = LoggerFactory().get_logger(__name__)
    insert_factory = InsertFactory()
    
    try:
        insert_factory.insert_all_project_data()
        logger.info("🎉 All data inserted successfully!")

    except Exception as e:
        logger.error(f"❌ Error during data insertion: {e}")
        raise

if __name__ == "__main__":
    main()
