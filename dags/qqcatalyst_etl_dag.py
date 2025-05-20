from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os
import logging

# Add ETL package to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.api_client import QQCatalystClient
from extract.extractor import QQCatalystExtractor
from config.database import SessionLocal, engine

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def get_api_client():
    """Create QQCatalyst API client with OAuth 2.0 credentials from Airflow Variables"""
    client_id = Variable.get("QQCATALYST_CLIENT_ID")
    client_secret = Variable.get("QQCATALYST_CLIENT_SECRET")
    username = Variable.get("QQCATALYST_USERNAME")
    password = Variable.get("QQCATALYST_PASSWORD")
    base_url = Variable.get("QQCATALYST_BASE_URL", "https://api.qqcatalyst.com/v1")
    token_url = Variable.get("QQCATALYST_TOKEN_URL", "https://api.qqcatalyst.com/oauth/token")
    
    return QQCatalystClient(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        base_url=base_url,
        token_url=token_url
    )

async def extract_resource(resource: str, **context):
    """Extract a single resource from QQCatalyst API"""
    client = get_api_client()
    db = SessionLocal()
    
    try:
        extractor = QQCatalystExtractor(client, db)
        
        # Get location_id for resources that need it
        location_id = None
        if resource in ['fees', 'signatures']:
            location_id = Variable.get("QQCATALYST_DEFAULT_LOCATION_ID", "")
            
        count = await extractor.extract_resource(
            resource,
            last_modified_start=context['execution_date'].isoformat(),
            location_id=location_id
        )
        logger.info(f"Extracted {count} records for {resource}")
        return count
    finally:
        db.close()

with DAG(
    'qqcatalyst_etl',
    default_args=default_args,
    description='ETL pipeline for QQCatalyst data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 2, 10),
    catchup=False,
    tags=['qqcatalyst', 'etl'],
) as dag:
    
    # Create extract tasks for each resource type
    resources = [
        # Core resources
        'contacts',
        'policies',
        'quotes',
        'claims',
        'documents',
        'applications',
        'renewals',
        'terminations',
        'billing_records',
        'commission_rules',
        'commissions',
        'fees',
        'acord_forms',
        
        # Additional resources
        'tasks',
        'text_messages',
        'carriers',
        'commercial_auto_drivers',
        'commercial_auto_vehicles',
        'employees',
        'locations',
        'notes',
        'signatures',
        'social_media',
        'websites',
        'underwriting_questions'
    ]
    
    extract_tasks = {}
    for resource in resources:
        task_id = f'extract_{resource}'
        extract_tasks[resource] = PythonOperator(
            task_id=task_id,
            python_callable=extract_resource,
            op_kwargs={'resource': resource},
            provide_context=True,
        )
    
    # Set up dependencies based on API relationships
    
    # 1. Core entity dependencies
    contacts_task = extract_tasks['contacts']
    locations_task = extract_tasks['locations']
    employees_task = extract_tasks['employees']
    
    # Policies depend on contacts
    policies_task = extract_tasks['policies']
    contacts_task >> policies_task
    
    # Quotes depend on contacts
    quotes_task = extract_tasks['quotes']
    contacts_task >> quotes_task
    
    # Claims depend on policies
    claims_task = extract_tasks['claims']
    policies_task >> claims_task
    
    # Documents can be extracted after contacts
    documents_task = extract_tasks['documents']
    contacts_task >> documents_task
    
    # Applications depend on contacts
    applications_task = extract_tasks['applications']
    contacts_task >> applications_task
    
    # Renewals depend on policies
    renewals_task = extract_tasks['renewals']
    policies_task >> renewals_task
    
    # Terminations depend on policies
    terminations_task = extract_tasks['terminations']
    policies_task >> terminations_task
    
    # Billing records depend on policies
    billing_records_task = extract_tasks['billing_records']
    policies_task >> billing_records_task
    
    # Commission rules can be extracted independently
    commission_rules_task = extract_tasks['commission_rules']
    
    # Commissions depend on policies and commission rules
    commissions_task = extract_tasks['commissions']
    policies_task >> commissions_task
    commission_rules_task >> commissions_task
    
    # Fees depend on policies and locations
    fees_task = extract_tasks['fees']
    policies_task >> fees_task
    locations_task >> fees_task
    
    # ACORD forms depend on policies
    acord_forms_task = extract_tasks['acord_forms']
    policies_task >> acord_forms_task
    
    # 2. Additional resource dependencies
    
    # Tasks depend on contacts and employees
    tasks_task = extract_tasks['tasks']
    contacts_task >> tasks_task
    employees_task >> tasks_task
    
    # Text messages depend on contacts
    text_messages_task = extract_tasks['text_messages']
    contacts_task >> text_messages_task
    
    # Commercial auto details depend on policies
    commercial_auto_drivers_task = extract_tasks['commercial_auto_drivers']
    commercial_auto_vehicles_task = extract_tasks['commercial_auto_vehicles']
    policies_task >> commercial_auto_drivers_task
    policies_task >> commercial_auto_vehicles_task
    
    # Notes can be extracted after contacts
    notes_task = extract_tasks['notes']
    contacts_task >> notes_task
    
    # Signatures depend on policies and locations
    signatures_task = extract_tasks['signatures']
    policies_task >> signatures_task
    locations_task >> signatures_task
    
    # Social media and websites depend on contacts
    social_media_task = extract_tasks['social_media']
    websites_task = extract_tasks['websites']
    contacts_task >> social_media_task
    contacts_task >> websites_task
    
    # Underwriting questions depend on policies
    underwriting_questions_task = extract_tasks['underwriting_questions']
    policies_task >> underwriting_questions_task 