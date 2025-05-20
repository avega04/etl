# QQCatalyst ETL Pipeline

This ETL (Extract, Transform, Load) pipeline synchronizes data from the QQCatalyst API to a local PostgreSQL database. The pipeline is designed for reliability, data consistency, and supports incremental loading with idempotent operations.

## Architecture

The pipeline is structured into three main components:

### 1. Extract
- Handles API communication with QQCatalyst
- Implements retry logic and error handling
- Stores raw JSON responses in staging tables
- Supports incremental extraction based on modification dates
- Tracks extraction batches for resumability

### 2. Transform
- Converts raw JSON data into structured format
- Implements comprehensive data validation:
  - Email format validation
  - Phone number standardization
  - ZIP code validation
  - US state code validation
  - Currency amount validation
  - Date format validation
  - Status value validation
  - UUID validation
- Provides data cleaning and normalization:
  - Text field normalization
  - Phone number formatting
  - Email address standardization
  - Currency amount formatting
- Tracks validation errors separately from other errors
- Maintains data integrity through proper error handling

### 3. Load
- Loads transformed data into production tables
- Implements idempotent upsert operations
- Supports incremental loading with sync windows
- Prevents duplicate records through unique constraints
- Tracks sync history and batch status

## Data Models

### Staging Models
Raw data is stored in staging tables that mirror the API response structure:
- raw_contacts
- raw_policies
- raw_quotes
- raw_claims
- raw_applications
- raw_renewals
- raw_terminations
- raw_billing_records
- raw_acord_forms

### Production Models
Transformed data is stored in normalized production tables:

1. Core Entities:
   - agencies
   - locations
   - employees
   - departments
   - contacts
   - carriers

2. Insurance Products:
   - quotes
   - applications
   - policies
   - policy_details
   - drivers
   - vehicles
   - coverages
   - equipment

3. Claims & Renewals:
   - claims
   - renewals
   - terminations

4. Financial:
   - billing_records
   - commission_rules
   - commissions
   - fees

5. Documents:
   - documents
   - acord_forms

## Unique Constraints

Each entity type has defined unique constraints to prevent duplicates:

```python
UNIQUE_CONSTRAINTS = {
    Contact: ['source_id'],
    Policy: ['source_id', 'policy_number'],
    Quote: ['source_id'],
    Application: ['source_id', 'quote_id'],
    Claim: ['source_id', 'claim_number'],
    Renewal: ['source_id', 'policy_id'],
    Termination: ['source_id', 'policy_id'],
    BillingRecord: ['source_id', 'billing_date', 'contact_id'],
    AcordForm: ['source_id', 'api_form_id'],
    Commission: ['source_id', 'policy_id'],
    Fee: ['source_id', 'policy_id', 'fee_type']
}
```

## Sync Windows

The pipeline supports time-based synchronization windows to handle incremental updates:

- Each sync operation requires a time window (start_date, end_date)
- Records are only updated if:
  1. They've never been synced before
  2. They were last synced before the current window's end date
  3. They've been updated within the current sync window
- Sync window information is stored with each batch
- Prevents duplicate processing of unchanged records

Example usage:
```python
sync_window = {
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 1, 31)
}
load_service.load_batch(batch_id, sync_window)
```

## Batch Processing

The ETL pipeline processes data in batches:

1. Extract Phase:
   - Creates a new batch record
   - Extracts data from API
   - Stores raw data in staging tables
   - Marks batch as 'extracted'

2. Transform Phase:
   - Validates and cleans data
   - Transforms into structured format
   - Tracks validation errors
   - Marks batch as 'transformed'

3. Load Phase:
   - Loads data into production tables
   - Updates existing records based on sync window
   - Tracks sync status
   - Marks batch as 'completed'

## Error Handling

The pipeline implements comprehensive error handling:

1. Validation Errors:
   - Invalid data format
   - Missing required fields
   - Invalid status values
   - Records marked as 'validation_error'

2. Processing Errors:
   - API communication errors
   - Database errors
   - Transaction failures
   - Records marked as 'error'

3. Error Recovery:
   - Failed batches can be retried
   - Partial batch completion is handled
   - Error messages are preserved for debugging

## Usage

1. Initialize the services:
```python
from etl.extract import ExtractService
from etl.transform import TransformService
from etl.load import LoadService

extract_service = ExtractService(db)
transform_service = TransformService(db)
load_service = LoadService(db)
```

2. Run a full sync:
```python
# Create sync window
sync_window = {
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 1, 31)
}

# Extract data
batch_id = extract_service.extract_data(sync_window)

# Transform data
transform_results = transform_service.transform_batch(batch_id)

# Load data
load_results = load_service.load_batch(batch_id, sync_window)
```

3. Check results:
```python
print(f"Transformed records: {transform_results}")
print(f"Loaded records: {load_results}")
```

## Dependencies

- Python 3.8+
- PostgreSQL 12+
- SQLAlchemy
- Requests
- Python-dateutil
- Logging

## Configuration

Configuration is managed through environment variables:

```bash
# Database
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/qqcatalyst_etl

# QQCatalyst API OAuth 2.0 Credentials
QQCATALYST_CLIENT_ID=your_client_id
QQCATALYST_CLIENT_SECRET=your_client_secret
QQCATALYST_USERNAME=your_username
QQCATALYST_PASSWORD=your_password
QQCATALYST_API_URL=https://api.qqcatalyst.com/v1
QQCATALYST_TOKEN_URL=https://api.qqcatalyst.com/oauth/token

# Logging
LOG_LEVEL=INFO
LOG_FILE=/var/log/etl/qqcatalyst.log
```

## Project Structure

```
etl/
├── alembic/              # Database migrations
├── config/               # Configuration files
├── dags/                 # Airflow DAG definitions
├── extract/              # Data extraction service
├── models/              # SQLAlchemy models
├── tests/               # Test files
└── README.md            # This file
```

## Database Schema

The raw schema contains tables for each QQCatalyst resource:

- raw_contacts
- raw_policies
- raw_quotes
- raw_claims
- raw_documents
- raw_applications
- raw_renewals
- raw_terminations
- raw_billing_records
- raw_commission_rules
- raw_commissions
- raw_fees
- raw_acord_forms

Each table contains:
- `id`: UUID primary key
- `created_at`: Timestamp of record creation
- `updated_at`: Timestamp of last update
- `raw_data`: JSON data from API
- `source_id`: Original ID from QQCatalyst
- `etl_batch_id`: UUID for tracking ETL runs
- `status`: Processing status
- `error_message`: Error details if any
- `retry_count`: Number of retry attempts

## ETL Process

1. **Extraction**:
   - Fetches data from QQCatalyst API
   - Handles pagination and rate limiting
   - Supports incremental loads based on last modified date

2. **Loading**:
   - Stores raw JSON in PostgreSQL
   - Tracks ETL metadata
   - Handles errors and retries

3. **Orchestration**:
   - Airflow DAG runs hourly
   - Resources extracted in dependency order
   - Proper error handling and notifications

## Development

1. Run tests:
```bash
pytest tests/
```

2. Create new migration:
```bash
alembic revision -m "description"
```

3. Apply migrations:
```bash
alembic upgrade head
```

## Monitoring

- Check Airflow UI for DAG status
- Monitor database tables for record counts
- Check logs for error messages

## Error Handling

- API errors are retried with exponential backoff
- Failed records are marked with error status
- Email notifications for critical failures

## Contributing

1. Create a feature branch
2. Make changes
3. Run tests
4. Submit pull request

## License

MIT 
