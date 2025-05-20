from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from .validators import DataValidator, ValidationError
from ..models.production import (
    Contact,
    Policy,
    Quote,
    Claim,
    Document,
    Application,
    Renewal,
    Termination,
    BillingRecord,
    CommissionRule,
    Commission,
    Fee,
    AcordForm
)

logger = logging.getLogger(__name__)

class TransformService:
    """Service for transforming raw JSON data into structured data"""
    
    def __init__(self, db: Session):
        self.db = db
        self.validator = DataValidator()
    
    def transform_contacts(self, batch_id: str) -> int:
        """
        Transform raw contacts into structured format with validation
        
        Args:
            batch_id: ETL batch ID to process
            
        Returns:
            Number of records transformed
        """
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_contacts 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                email = self.validator.validate_email(raw_data.get("email"))
                phone = self.validator.validate_phone(raw_data.get("phone"))
                zip_code = self.validator.validate_zip_code(raw_data.get("zipCode"))
                state = self.validator.validate_state(raw_data.get("state"))
                
                contact = Contact(
                    source_id=row.source_id,
                    first_name=self.validator.clean_text(raw_data.get("firstName")),
                    last_name=self.validator.clean_text(raw_data.get("lastName")),
                    email=email,
                    phone=phone,
                    address=self.validator.clean_text(raw_data.get("address")),
                    city=self.validator.clean_text(raw_data.get("city")),
                    state=state,
                    zip_code=zip_code,
                    contact_type=self.validator.validate_status(
                        raw_data.get("type"),
                        {"INDIVIDUAL", "BUSINESS"}
                    ),
                    status=self.validator.validate_status(
                        raw_data.get("status"),
                        {"ACTIVE", "INACTIVE", "PENDING"}
                    ),
                    created_at=self.validator.validate_date(raw_data.get("createdAt")),
                    updated_at=self.validator.validate_date(raw_data.get("updatedAt"))
                )
                self.db.add(contact)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_contacts SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for contact {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_contacts 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming contact {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_contacts 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed
    
    def transform_policies(self, batch_id: str) -> int:
        """
        Transform raw policies into structured format with validation
        
        Args:
            batch_id: ETL batch ID to process
            
        Returns:
            Number of records transformed
        """
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_policies 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                policy_number = self.validator.validate_policy_number(raw_data.get("policyNumber"))
                contact_id = self.validator.validate_uuid(raw_data.get("contactId"))
                premium = self.validator.validate_currency_amount(raw_data.get("premium"))
                
                policy = Policy(
                    source_id=row.source_id,
                    policy_number=policy_number,
                    contact_id=contact_id,
                    carrier=self.validator.clean_text(raw_data.get("carrier")),
                    policy_type=self.validator.clean_text(raw_data.get("type")),
                    status=self.validator.validate_status(
                        raw_data.get("status"),
                        {"QUOTED", "APPLIED", "BOUND", "ACTIVE", "EXPIRED", "TERMINATED"}
                    ),
                    effective_date=self.validator.validate_date(raw_data.get("effectiveDate")),
                    expiration_date=self.validator.validate_date(raw_data.get("expirationDate")),
                    premium=premium,
                    created_at=self.validator.validate_date(raw_data.get("createdAt")),
                    updated_at=self.validator.validate_date(raw_data.get("updatedAt"))
                )
                self.db.add(policy)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_policies SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for policy {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_policies 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming policy {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_policies 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed
    
    def transform_claims(self, batch_id: str) -> int:
        """Transform raw claims into structured format with validation"""
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_claims 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                policy_id = self.validator.validate_uuid(raw_data.get("policyId"))
                amount = self.validator.validate_currency_amount(raw_data.get("amount"))
                
                claim = Claim(
                    source_id=row.source_id,
                    claim_number=self.validator.clean_text(raw_data.get("claimNumber")),
                    policy_id=policy_id,
                    status=self.validator.validate_status(
                        raw_data.get("status"),
                        {"OPEN", "INVESTIGATING", "RESERVED", "PAID", "CLOSED"}
                    ),
                    incident_date=self.validator.validate_date(raw_data.get("incidentDate")),
                    report_date=self.validator.validate_date(raw_data.get("reportDate")),
                    description=self.validator.clean_text(raw_data.get("description")),
                    amount=amount,
                    created_at=self.validator.validate_date(raw_data.get("createdAt")),
                    updated_at=self.validator.validate_date(raw_data.get("updatedAt"))
                )
                self.db.add(claim)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_claims SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for claim {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_claims 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming claim {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_claims 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed
    
    def transform_quotes(self, batch_id: str) -> int:
        """Transform raw quotes into structured format with validation"""
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_quotes 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                contact_id = self.validator.validate_uuid(raw_data.get("contactId"))
                location_id = self.validator.validate_uuid(raw_data.get("locationId"))
                carrier_id = self.validator.validate_uuid(raw_data.get("carrierId"))
                
                # Extract policy reference from raw data but don't make it a required field
                policy_id = raw_data.get("PolicyId")
                if policy_id:
                    # Validate policy_id format if present
                    policy_id = self.validator.validate_uuid(policy_id)
                
                # Create quote record with independent structure
                quote = Quote(
                    source_id=row.source_id,
                    contact_id=contact_id,
                    location_id=location_id,
                    carrier_id=carrier_id,
                    lob_type=self.validator.clean_text(raw_data.get("lineOfBusiness")),
                    quote_date=self.validator.validate_date(raw_data.get("quoteDate")),
                    valid_until=self.validator.validate_date(raw_data.get("validUntil")),
                    status=self.validator.validate_status(
                        raw_data.get("status"),
                        {"DRAFT", "ISSUED", "EXPIRED", "REVISED"}
                    ),
                    # Store policy reference in quote_data if present
                    quote_data={
                        **raw_data.get("quoteData", {}),
                        "policy_id": policy_id
                    } if policy_id else raw_data.get("quoteData"),
                    created_at=self.validator.validate_date(raw_data.get("createdAt"))
                )
                self.db.add(quote)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_quotes SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for quote {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_quotes 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming quote {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_quotes 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed

    def transform_applications(self, batch_id: str) -> int:
        """Transform raw applications into structured format with validation"""
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_applications 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                quote_id = self.validator.validate_uuid(raw_data.get("quoteId"))
                contact_id = self.validator.validate_uuid(raw_data.get("contactId"))
                
                application = Application(
                    source_id=row.source_id,
                    quote_id=quote_id,
                    contact_id=contact_id,
                    submitted_at=self.validator.validate_date(raw_data.get("submittedAt")),
                    status=self.validator.validate_status(
                        raw_data.get("status"),
                        {"PENDING", "UNDERWRITING", "APPROVED", "DECLINED"}
                    ),
                    application_data=raw_data.get("applicationData"),
                    created_at=self.validator.validate_date(raw_data.get("createdAt"))
                )
                self.db.add(application)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_applications SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for application {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_applications 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming application {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_applications 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed

    def transform_renewals(self, batch_id: str) -> int:
        """Transform raw renewals into structured format with validation"""
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_renewals 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                policy_id = self.validator.validate_uuid(raw_data.get("policyId"))
                premium = self.validator.validate_currency_amount(raw_data.get("premiumOffered"))
                
                renewal = Renewal(
                    source_id=row.source_id,
                    policy_id=policy_id,
                    offer_date=self.validator.validate_date(raw_data.get("offerDate")),
                    new_effective=self.validator.validate_date(raw_data.get("newEffective")),
                    new_expiration=self.validator.validate_date(raw_data.get("newExpiration")),
                    premium_offered=premium,
                    status=self.validator.validate_status(
                        raw_data.get("status"),
                        {"OFFERED", "ACCEPTED", "DECLINED"}
                    ),
                    renewal_data=raw_data.get("renewalData"),
                    created_at=self.validator.validate_date(raw_data.get("createdAt"))
                )
                self.db.add(renewal)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_renewals SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for renewal {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_renewals 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming renewal {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_renewals 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed

    def transform_terminations(self, batch_id: str) -> int:
        """Transform raw terminations into structured format with validation"""
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_terminations 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                policy_id = self.validator.validate_uuid(raw_data.get("policyId"))
                
                termination = Termination(
                    source_id=row.source_id,
                    policy_id=policy_id,
                    termination_date=self.validator.validate_date(raw_data.get("terminationDate")),
                    termination_type=self.validator.validate_status(
                        raw_data.get("terminationType"),
                        {"VOLUNTARY", "NONRENEWAL", "LAPSE", "CANCELLATION"}
                    ),
                    reason=self.validator.clean_text(raw_data.get("reason")),
                    notes=self.validator.clean_text(raw_data.get("notes"))
                )
                self.db.add(termination)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_terminations SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for termination {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_terminations 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming termination {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_terminations 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed

    def transform_billing_records(self, batch_id: str) -> int:
        """Transform raw billing records into structured format with validation"""
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_billing_records 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                contact_id = self.validator.validate_uuid(raw_data.get("contactId"))
                policy_id = self.validator.validate_uuid(raw_data.get("policyId"))
                amount = self.validator.validate_currency_amount(raw_data.get("amount"))
                
                billing = BillingRecord(
                    source_id=row.source_id,
                    contact_id=contact_id,
                    policy_id=policy_id,
                    billing_type=self.validator.clean_text(raw_data.get("billingType")),
                    amount=amount,
                    billing_date=self.validator.validate_date(raw_data.get("billingDate")),
                    created_at=self.validator.validate_date(raw_data.get("createdAt"))
                )
                self.db.add(billing)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_billing_records SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for billing record {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_billing_records 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming billing record {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_billing_records 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed

    def transform_acord_forms(self, batch_id: str) -> int:
        """Transform raw ACORD forms into structured format with validation"""
        query = text("""
            SELECT raw_data, source_id 
            FROM raw_acord_forms 
            WHERE etl_batch_id = :batch_id 
            AND status = 'pending'
        """)
        
        transformed = 0
        for row in self.db.execute(query, {"batch_id": batch_id}):
            try:
                raw_data = row.raw_data
                
                # Validate and clean data
                customer_id = self.validator.validate_uuid(raw_data.get("customerId"))
                policy_id = self.validator.validate_uuid(raw_data.get("policyId"))
                
                form = AcordForm(
                    source_id=row.source_id,
                    api_form_id=raw_data.get("apiFormId"),
                    customer_id=customer_id,
                    policy_id=policy_id,
                    template_id=raw_data.get("templateId"),
                    data=raw_data.get("formData"),
                    description=self.validator.clean_text(raw_data.get("description")),
                    created_at=self.validator.validate_date(raw_data.get("createdAt"))
                )
                self.db.add(form)
                transformed += 1
                
                self.db.execute(
                    text("UPDATE raw_acord_forms SET status = 'transformed' WHERE source_id = :source_id"),
                    {"source_id": row.source_id}
                )
                
            except ValidationError as e:
                logger.error(f"Validation error for ACORD form {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_acord_forms 
                        SET status = 'validation_error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
            except Exception as e:
                logger.error(f"Error transforming ACORD form {row.source_id}: {e}")
                self.db.execute(
                    text("""
                        UPDATE raw_acord_forms 
                        SET status = 'error',
                            error_message = :error 
                        WHERE source_id = :source_id
                    """),
                    {"source_id": row.source_id, "error": str(e)}
                )
        
        self.db.commit()
        return transformed

    def transform_batch(self, batch_id: str) -> Dict[str, int]:
        """
        Transform all raw data for a given batch with validation
        
        Args:
            batch_id: ETL batch ID to process
            
        Returns:
            Dictionary with counts of records transformed per type
        """
        try:
            # Validate batch_id
            batch_id = self.validator.validate_uuid(batch_id)
            
            results = {}
            
            # Transform in order of dependencies
            results['contacts'] = self.transform_contacts(batch_id)
            results['quotes'] = self.transform_quotes(batch_id)
            results['applications'] = self.transform_applications(batch_id)
            results['policies'] = self.transform_policies(batch_id)
            results['claims'] = self.transform_claims(batch_id)
            results['renewals'] = self.transform_renewals(batch_id)
            results['terminations'] = self.transform_terminations(batch_id)
            results['billing_records'] = self.transform_billing_records(batch_id)
            results['acord_forms'] = self.transform_acord_forms(batch_id)
            
            return results
            
        except ValidationError as e:
            logger.error(f"Validation error for batch {batch_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}")
            raise 