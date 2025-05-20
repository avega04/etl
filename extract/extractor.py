from typing import List, Optional, Dict, Union
import logging
import uuid
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from extract.api_client import QQCatalystClient
from models.raw_models import (
    RawContact,
    RawPolicy
)
from sqlalchemy.sql import text
import asyncio

logger = logging.getLogger(__name__)

class QQCatalystExtractor:
    """Service for extracting data from QQCatalyst API to raw database tables"""
    
    RESOURCE_MODEL_MAP = {
        # Core resources
        "contacts": RawContact,
        "policies": RawPolicy
    }
    
    # Map resource names to their API endpoints
    RESOURCE_ENDPOINT_MAP = {
        # Core Entities
        "contacts": "Contacts/LastModifiedCreated",
        "policies": "Policies/LastModifiedCreated"
    }
    
    def __init__(
        self,
        api_client: QQCatalystClient,
        session: Session,
        batch_size: int = 1000
    ):
        """
        Initialize extractor
        
        Args:
            api_client: Initialized QQCatalyst API client
            session: SQLAlchemy database session
            batch_size: Number of records to buffer before bulk insert
        """
        self.api_client = api_client
        self.session = session
        self.batch_size = batch_size
        self.batch_id = str(uuid.uuid4())
        
    def _validate_record(self, record: dict, resource: str) -> List[str]:
        """
        Validate a record before saving to database
        
        Args:
            record: Raw record data from API
            resource: Resource type being validated
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Map of resource types to their ID field names
        id_field_map = {
            "contacts": ["EntityID", "ContactId"],
            "policies": ["EntityID", "PolicyId"]
        }
        
        # Get the valid ID fields for this resource
        valid_id_fields = id_field_map.get(resource, ["EntityID"])
        
        # Check for at least one valid ID field
        has_valid_id = any(record.get(field) for field in valid_id_fields)
        if not has_valid_id:
            errors.append(f"Missing required ID field. Expected one of: {', '.join(valid_id_fields)}")
            
        # Resource-specific validations
        if resource == "policies":
            # Check policy-specific fields
            if not record.get("PolicyNumber"):
                errors.append("Policy must have a policy number")
            if not record.get("Status"):
                errors.append("Policy must have a status")
                
        # Log validation results
        if errors:
            identifier = None
            for field in valid_id_fields:
                if record.get(field):
                    identifier = record.get(field)
                    break
                    
            logger.warning(f"Validation errors for {resource} record {identifier}:")
            for error in errors:
                logger.warning(f"  - {error}")
                
        return errors

    async def extract_resource(
        self,
        resource: str,
        last_modified_start: Optional[str] = None,
        last_modified_end: Optional[str] = None,
        test_mode: bool = False,
        limit: Optional[int] = None,
        **kwargs
    ) -> Union[int, List[Dict]]:
        """
        Extract data for a specific resource and store in raw table
        
        Args:
            resource: Resource type to extract (e.g., "contacts")
            last_modified_start: ISO timestamp for incremental extraction (start date)
            last_modified_end: ISO timestamp for incremental extraction (end date)
            test_mode: If True, return records instead of writing to DB
            limit: Maximum number of records to return in test mode
            **kwargs: Additional parameters for endpoint URLs (e.g., location_id, contact_id)
            
        Returns:
            Number of records extracted (int) or list of records (List[Dict]) in test mode
        """
        if resource not in self.RESOURCE_MODEL_MAP:
            raise ValueError(f"Unsupported resource type: {resource}")
            
        model_class = self.RESOURCE_MODEL_MAP[resource]
        endpoint = self.RESOURCE_ENDPOINT_MAP[resource].format(**kwargs)
        records_processed = 0
        buffer = []
        validation_errors = 0
        processed_ids = set()  # Track processed EntityIDs to detect duplicates
        
        # In test mode, we'll collect records to return
        test_records = []
        
        # Check for existing records in database before extraction
        if not test_mode:
            try:
                # Get list of existing source_ids from this resource table
                existing_source_ids_query = f"SELECT source_id FROM {model_class.__tablename__}"
                result = self.session.execute(text(existing_source_ids_query))
                existing_source_ids = set([row[0] for row in result.fetchall()])
                logger.info(f"Found {len(existing_source_ids)} existing records in {model_class.__tablename__}")
            except Exception as e:
                logger.error(f"Error checking existing records: {e}")
                existing_source_ids = set()
        else:
            existing_source_ids = set()
        
        # Track debugging statistics
        stats = {
            "total_items": 0,
            "skipped_validation": 0,
            "already_in_database": 0,
            "added_to_buffer": 0
        }
        
        async for page in self.api_client.get_paginated_resource(
            endpoint,
            last_modified_start=last_modified_start,
            last_modified_end=last_modified_end
        ):
            # Handle both dict and list responses
            items = []
            if isinstance(page, dict):
                items = page.get("Data", [])
            elif isinstance(page, list):
                items = page
            else:
                items = []
                
            stats["total_items"] += len(items)
            logger.info(f"Processing {len(items)} items from page {getattr(page, 'PageNumber', '?')}/{getattr(page, 'PagesTotal', '?')}")
            
            for item in items:
                # Use PolicyId as EntityID for policies if EntityID is missing
                entity_id = None
                if resource == "policies" and not item.get("EntityID") and item.get("PolicyId"):
                    entity_id = str(item.get("PolicyId"))
                    logger.debug(f"Using PolicyId as EntityID for policy: {entity_id}")
                else:
                    entity_id = str(item.get("EntityID", ""))
                
                if not entity_id:
                    logger.warning(f"Skipping record with no EntityID: {item}")
                    stats["skipped_validation"] += 1
                    continue
                
                # Check if already processed in this batch (duplicate in API response)
                if entity_id in processed_ids:
                    logger.debug(f"Skipping duplicate EntityID in API response: {entity_id}")
                    continue
                    
                processed_ids.add(entity_id)
                
                # Check if record already exists in database
                if entity_id in existing_source_ids:
                    stats["already_in_database"] += 1
                    if resource == "policies":
                        # Add more detailed logging for this resource we're investigating
                        identifier = item.get("PolicyNumber")
                        logger.debug(f"Skipping existing {resource} record: ID={entity_id}, {identifier}")
                    continue
                
                # Validate record before processing
                errors = self._validate_record(item, resource)
                if errors:
                    validation_errors += 1
                    stats["skipped_validation"] += 1
                    continue
                
                if test_mode:
                    test_records.append(item)
                    if limit and len(test_records) >= limit:
                        return test_records
                else:
                    raw_record = model_class(
                        source_id=entity_id,
                        raw_data=item,
                        etl_batch_id=self.batch_id
                    )
                    buffer.append(raw_record)
                    stats["added_to_buffer"] += 1
                    records_processed += 1
                    
                    if len(buffer) >= self.batch_size:
                        self._flush_buffer(buffer)
                        buffer = []
            
            # In test mode, if we have a limit and we've reached it, return early
            if test_mode and limit and len(test_records) >= limit:
                break
                    
        if not test_mode and buffer:  # Flush any remaining records
            self._flush_buffer(buffer)
            
        if validation_errors > 0:
            logger.warning(f"Completed with {validation_errors} validation errors out of {records_processed + validation_errors} total records")
            
        # Log summary statistics
        logger.info(f"Extraction stats for {resource}: {stats}")
        
        return test_records if test_mode else records_processed
        
    def _flush_buffer(self, buffer: List[any]) -> None:
        """
        Flush buffered records to database
        
        Args:
            buffer: List of model instances to insert
        """
        try:
            logger.info(f"Attempting to flush {len(buffer)} records to database")
            self.session.bulk_save_objects(buffer)
            self.session.commit()
            logger.info(f"Successfully flushed {len(buffer)} records to database")
        except Exception as e:
            self.session.rollback()
            # Log detailed error information
            logger.error(f"Database error while flushing buffer: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            
            # Log problematic records
            for i, record in enumerate(buffer):
                try:
                    # Try to serialize the record to identify issues
                    logger.error(f"Record {i}: source_id={record.source_id}, "
                               f"etl_batch_id={record.etl_batch_id}")
                except Exception as serialize_error:
                    logger.error(f"Error serializing record {i}: {str(serialize_error)}")
            
            # Re-raise with more context
            raise Exception(f"Failed to flush buffer to database: {str(e)}") from e
            
    async def extract_policy_dependent_resource(
        self,
        resource: str,
        policy_id: str,
        last_modified_start: Optional[str] = None,
        last_modified_end: Optional[str] = None
    ) -> int:
        """
        Extract data for a resource that depends on a policy ID
        
        Args:
            resource: Resource type to extract (e.g., "quotes")
            policy_id: Policy ID to fetch dependent resources for
            last_modified_start: ISO timestamp for incremental extraction (start date)
            last_modified_end: ISO timestamp for incremental extraction (end date)
            
        Returns:
            Number of records extracted
        """
        if resource not in self.RESOURCE_MODEL_MAP:
            raise ValueError(f"Unsupported resource type: {resource}")
            
        model_class = self.RESOURCE_MODEL_MAP[resource]
        endpoint = self.RESOURCE_ENDPOINT_MAP[resource].format(policy_id=policy_id)
        records_processed = 0
        buffer = []
        validation_errors = 0
        processed_ids = set()
        
        # Check for existing records in database before extraction
        try:
            existing_source_ids_query = f"SELECT source_id FROM {model_class.__tablename__}"
            result = self.session.execute(text(existing_source_ids_query))
            existing_source_ids = set([row[0] for row in result.fetchall()])
            logger.info(f"Found {len(existing_source_ids)} existing records in {model_class.__tablename__}")
        except Exception as e:
            logger.error(f"Error checking existing records: {e}")
            existing_source_ids = set()
        
        # Track debugging statistics
        stats = {
            "total_items": 0,
            "skipped_validation": 0,
            "already_in_database": 0,
            "added_to_buffer": 0
        }
        
        # Add rate limiting delay between API calls
        rate_limit_delay = 1.0  # 1 second between calls
        
        async for page in self.api_client.get_paginated_resource(
            endpoint,
            last_modified_start=last_modified_start,
            last_modified_end=last_modified_end
        ):
            # Add delay between API calls
            await asyncio.sleep(rate_limit_delay)
            
            # Handle both dict and list responses
            if isinstance(page, dict):
                items = page.get("Data", [])
            elif isinstance(page, list):
                items = page
            else:
                items = []
            stats["total_items"] += len(items)
            logger.info(f"Processing {len(items)} items from page {getattr(page, 'PageNumber', '?')}/{getattr(page, 'PagesTotal', '?')}")
            
            for item in items:
                # For quotes, we'll use a composite key of policy_id and quote_id if available
                if resource == "quotes":
                    # Check for all possible quote ID variations
                    quote_id = (
                        item.get("QuoteId") or 
                        item.get("QuoteID") or 
                        item.get("EntityID")
                    )
                    if not quote_id:
                        logger.warning(f"Skipping quote with no QuoteId/QuoteID/EntityID: {item}")
                        stats["skipped_validation"] += 1
                        continue
                    entity_id = f"{policy_id}_{quote_id}"
                    
                    # Add policy reference to the raw data
                    if isinstance(item, dict):
                        item["PolicyId"] = policy_id
                else:
                    entity_id = str(item.get("EntityID", ""))
                
                if not entity_id:
                    logger.warning(f"Skipping record with no EntityID: {item}")
                    stats["skipped_validation"] += 1
                    continue
                
                if entity_id in processed_ids:
                    logger.debug(f"Skipping duplicate EntityID in API response: {entity_id}")
                    continue
                    
                processed_ids.add(entity_id)
                
                if entity_id in existing_source_ids:
                    stats["already_in_database"] += 1
                    continue
                
                errors = self._validate_record(item, resource)
                if errors:
                    validation_errors += 1
                    stats["skipped_validation"] += 1
                    continue
                    
                raw_record = model_class(
                    source_id=entity_id,
                    raw_data=item,
                    etl_batch_id=self.batch_id
                )
                buffer.append(raw_record)
                stats["added_to_buffer"] += 1
                records_processed += 1
                
                if len(buffer) >= self.batch_size:
                    self._flush_buffer(buffer)
                    buffer = []
                    
        if buffer:  # Flush any remaining records
            self._flush_buffer(buffer)
            
        if validation_errors > 0:
            logger.warning(f"Completed with {validation_errors} validation errors out of {records_processed + validation_errors} total records")
            
        logger.info(f"Extraction stats for {resource} (policy {policy_id}): {stats}")
        
        return records_processed

    def extract_all_resources(self, start_date: datetime, end_date: datetime) -> Dict[str, int]:
        """
        Extract all resources from the API within the date range
        
        Args:
            start_date: Start date for extraction
            end_date: End date for extraction
            
        Returns:
            Dictionary with counts of records added for each resource
        """
        results = {}
        
        # 1. Extract base entities first
        base_resources = [
            "contacts",
            "policies"
        ]
        
        for resource in base_resources:
            try:
                count = self.extract_resource(resource, start_date, end_date)
                results[resource] = count
                logger.info(f"Extracted {count} {resource} records")
            except Exception as e:
                logger.error(f"Error extracting {resource}: {str(e)}")
                results[resource] = 0
                
        # 2. Extract policy-dependent resources
        policy_resources = [
            "quotes"
        ]
        
        for resource in policy_resources:
            try:
                count = self.extract_policy_dependent_resource(resource, start_date, end_date)
                results[resource] = count
                logger.info(f"Extracted {count} {resource} records")
            except Exception as e:
                logger.error(f"Error extracting {resource}: {str(e)}")
                results[resource] = 0
                
        return results 