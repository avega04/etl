import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent.absolute())
sys.path.insert(0, project_root)

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from extract.api_client import QQCatalystClient
from extract.extractor import QQCatalystExtractor
from models.raw_models import RawPolicy, RawContact

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def test_flush_buffer(session, extractor):
    """Test if the _flush_buffer method works correctly"""
    # Create a test record for raw_contacts
    test_contact = RawContact(
        source_id="test_contact_123",
        raw_data={"EntityID": "test_contact_123", "DisplayName": "Test Contact", "FirstName": "Test", "LastName": "Contact"},
        etl_batch_id=extractor.batch_id
    )
    
    # Create a test record for raw_policies
    test_policy = RawPolicy(
        source_id="test_policy_456",
        raw_data={"EntityID": "test_policy_456", "PolicyNumber": "TEST-123", "Status": "Active"},
        etl_batch_id=extractor.batch_id
    )
    
    # Test flushing each buffer separately
    extractor._flush_buffer([test_contact])
    extractor._flush_buffer([test_policy])
    
    logger.info("Test flush buffer completed successfully")
    
    # Verify records were actually inserted
    for table, model_class in [("raw_contacts", RawContact), ("raw_policies", RawPolicy)]:
        result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()
        logger.info(f"Count in {table}: {count}")

async def test_resource_extraction(extractor, resource, start_date, end_date):
    """Test extraction for a specific resource"""
    logger.info(f"\n{'='*50}\nTesting extraction for {resource}\n{'='*50}")
    
    try:
        # Get existing record count
        model_class = extractor.RESOURCE_MODEL_MAP[resource]
        count_before = extractor.session.query(model_class).count()
        logger.info(f"Records in {model_class.__tablename__} BEFORE extraction: {count_before}")
        
        # Extract the resource with page size 10
        endpoint = extractor.RESOURCE_ENDPOINT_MAP[resource]
        params = {
            'pageNumber': 1,
            'pageSize': 10,
            'startDate': start_date.isoformat(),
            'endDate': end_date.isoformat()
        }
        
        # Make the API request
        logger.info(f"Making API request to {endpoint} with params: {params}")
        response = await extractor.api_client.get_resource(endpoint, params)
        
        if not response or not response.get("Data"):
            logger.warning(f"No data returned for {resource}")
            return 0
            
        items = response["Data"]
        logger.info(f"Retrieved {len(items)} records for {resource}")
        
        # Process the items
        buffer = []
        processed = 0
        
        for item in items:
            # Get source_id based on resource type
            source_id = str(item.get("EntityID") or item.get("PolicyId") or 
                          item.get("ContactId") or "unknown")
            
            raw_record = model_class(
                source_id=source_id,
                raw_data=item,
                etl_batch_id=extractor.batch_id
            )
            
            buffer.append(raw_record)
            processed += 1
            
            # Flush the buffer when it reaches the batch size
            if len(buffer) >= extractor.batch_size:
                extractor._flush_buffer(buffer)
                buffer = []
        
        # Flush any remaining records
        if buffer:
            extractor._flush_buffer(buffer)
        
        # Get new count
        count_after = extractor.session.query(model_class).count()
        records_added = count_after - count_before
        
        logger.info(f"Records in {model_class.__tablename__} AFTER extraction: {count_after}")
        logger.info(f"Records added: {records_added}")
        logger.info(f"Records processed: {processed}")
        
        return records_added
    except Exception as e:
        logger.error(f"Error testing {resource} extraction: {e}")
        logger.error(f"Exception type: {type(e)}")
        return 0

async def main():
    # Initialize API client
    client = QQCatalystClient()
    
    # Initialize database connection
    engine = create_engine(os.getenv("DATABASE_URL"))
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Initialize extractor
    extractor = QQCatalystExtractor(client, session)
    
    # Test flush buffer
    await test_flush_buffer(session, extractor)
    
    # Get date range for testing
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)
    
    # Test only policies and contacts
    resources = ["policies", "contacts"]
    
    for resource in resources:
        await test_resource_extraction(extractor, resource, start_date, end_date)
    
    # Close database connection
    session.close()

if __name__ == "__main__":
    asyncio.run(main()) 