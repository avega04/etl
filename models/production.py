from sqlalchemy import Column, String, Text, UUID, DateTime, Date, Numeric, Integer, Boolean, ForeignKey, CheckConstraint, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from ..config.database import Base
import uuid

class Agency(Base):
    __tablename__ = 'agencies'
    
    agency_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    locations = relationship("Location", back_populates="agency")

class Location(Base):
    __tablename__ = 'locations'
    
    location_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    agency_id = Column(UUID, ForeignKey('agencies.agency_id'), nullable=False)
    name = Column(Text)
    address_id = Column(UUID)
    phone = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    agency = relationship("Agency", back_populates="locations")
    employees = relationship("Employee", back_populates="location")

class Employee(Base):
    __tablename__ = 'employees'
    
    employee_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    location_id = Column(UUID, ForeignKey('locations.location_id'), nullable=False)
    first_name = Column(Text, nullable=False)
    last_name = Column(Text, nullable=False)
    email = Column(Text, unique=True)
    role = Column(Text)
    status = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    location = relationship("Location", back_populates="employees")

class Department(Base):
    __tablename__ = 'departments'
    
    department_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    location_id = Column(UUID, ForeignKey('locations.location_id'), nullable=False)
    name = Column(Text, nullable=False)

class Contact(Base):
    __tablename__ = 'contacts'
    
    contact_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    type = Column(Text, CheckConstraint("type IN ('Individual','Business')"), nullable=False)
    first_name = Column(Text)
    last_name = Column(Text)
    company_name = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    addresses = relationship("ContactAddress", back_populates="contact")
    emails = relationship("ContactEmail", back_populates="contact")
    phones = relationship("ContactPhone", back_populates="contact")

class ContactAddress(Base):
    __tablename__ = 'contact_addresses'
    
    address_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID, ForeignKey('contacts.contact_id'), nullable=False)
    type = Column(Text)
    line1 = Column(Text)
    city = Column(Text)
    state = Column(Text)
    zip = Column(Text)
    country = Column(Text)
    
    contact = relationship("Contact", back_populates="addresses")

class ContactEmail(Base):
    __tablename__ = 'contact_emails'
    
    email_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID, ForeignKey('contacts.contact_id'), nullable=False)
    email = Column(Text, nullable=False)
    is_primary = Column(Boolean, default=False)
    
    contact = relationship("Contact", back_populates="emails")

class ContactPhone(Base):
    __tablename__ = 'contact_phones'
    
    phone_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID, ForeignKey('contacts.contact_id'), nullable=False)
    number = Column(Text, nullable=False)
    type = Column(Text)
    is_primary = Column(Boolean, default=False)
    
    contact = relationship("Contact", back_populates="phones")

class Carrier(Base):
    __tablename__ = 'carriers'
    
    carrier_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    parent_id = Column(UUID, ForeignKey('carriers.carrier_id'))
    rating = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Quote(Base):
    __tablename__ = 'quotes'
    
    quote_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID, ForeignKey('contacts.contact_id'), nullable=False)
    location_id = Column(UUID, ForeignKey('locations.location_id'), nullable=False)
    carrier_id = Column(UUID, ForeignKey('carriers.carrier_id'), nullable=False)
    lob_type = Column(Text, nullable=False)
    quote_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    valid_until = Column(DateTime(timezone=True))
    status = Column(Text, CheckConstraint("status IN ('Draft','Issued','Expired','Revised')"), nullable=False)
    quote_data = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Application(Base):
    __tablename__ = 'applications'
    
    application_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    quote_id = Column(UUID, ForeignKey('quotes.quote_id'), nullable=False)
    contact_id = Column(UUID, ForeignKey('contacts.contact_id'), nullable=False)
    submitted_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(Text, CheckConstraint("status IN ('Pending','Underwriting','Approved','Declined')"), nullable=False)
    application_data = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Policy(Base):
    __tablename__ = 'policies'
    
    policy_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    policy_number = Column(Text, unique=True, nullable=False)
    contact_id = Column(UUID, ForeignKey('contacts.contact_id'), nullable=False)
    location_id = Column(UUID, ForeignKey('locations.location_id'), nullable=False)
    carrier_id = Column(UUID, ForeignKey('carriers.carrier_id'), nullable=False)
    line_of_business = Column(Text)
    status = Column(Text, CheckConstraint("status IN ('Quoted','Applied','Bound','Active','Expired','Terminated')"), nullable=False)
    effective_date = Column(Date)
    expiration_date = Column(Date)
    total_premium = Column(Numeric(12,2))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    details = relationship("PolicyDetail", back_populates="policy")
    claims = relationship("Claim", back_populates="policy")
    renewals = relationship("Renewal", back_populates="policy")

class PolicyDetail(Base):
    __tablename__ = 'policy_details'
    
    detail_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID, ForeignKey('policies.policy_id'), nullable=False)
    lob_type = Column(Text, nullable=False)
    detail_data = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    policy = relationship("Policy", back_populates="details")
    drivers = relationship("Driver", back_populates="detail")
    vehicles = relationship("Vehicle", back_populates="detail")
    coverages = relationship("Coverage", back_populates="detail")
    equipment = relationship("Equipment", back_populates="detail")

class Driver(Base):
    __tablename__ = 'drivers'
    
    driver_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    detail_id = Column(UUID, ForeignKey('policy_details.detail_id'), nullable=False)
    name = Column(Text)
    license_number = Column(Text)
    dob = Column(Date)
    
    detail = relationship("PolicyDetail", back_populates="drivers")

class Vehicle(Base):
    __tablename__ = 'vehicles'
    
    vehicle_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    detail_id = Column(UUID, ForeignKey('policy_details.detail_id'), nullable=False)
    vin = Column(Text)
    make = Column(Text)
    model = Column(Text)
    year = Column(Integer)
    
    detail = relationship("PolicyDetail", back_populates="vehicles")

class Coverage(Base):
    __tablename__ = 'coverages'
    
    coverage_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    detail_id = Column(UUID, ForeignKey('policy_details.detail_id'), nullable=False)
    code = Column(Text)
    limit_amount = Column(Numeric(12,2))
    deductible = Column(Numeric(12,2))
    
    detail = relationship("PolicyDetail", back_populates="coverages")

class Equipment(Base):
    __tablename__ = 'equipment'
    
    equipment_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    detail_id = Column(UUID, ForeignKey('policy_details.detail_id'), nullable=False)
    description = Column(Text)
    value = Column(Numeric(12,2))
    
    detail = relationship("PolicyDetail", back_populates="equipment")

class Claim(Base):
    __tablename__ = 'claims'
    
    claim_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID, ForeignKey('policies.policy_id'), nullable=False)
    contact_id = Column(UUID)
    claim_number = Column(Text, unique=True)
    reported_date = Column(Date)
    status = Column(Text, CheckConstraint("status IN ('Open','Investigating','Reserved','Paid','Closed')"), nullable=False)
    claim_data = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    policy = relationship("Policy", back_populates="claims")

class Renewal(Base):
    __tablename__ = 'renewals'
    
    renewal_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID, ForeignKey('policies.policy_id'), nullable=False)
    offer_date = Column(Date, nullable=False)
    new_effective = Column(Date)
    new_expiration = Column(Date)
    premium_offered = Column(Numeric(12,2))
    status = Column(Text, CheckConstraint("status IN ('Offered','Accepted','Declined')"), nullable=False)
    renewal_data = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    policy = relationship("Policy", back_populates="renewals")

class Termination(Base):
    __tablename__ = 'terminations'
    
    termination_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID, ForeignKey('policies.policy_id'), nullable=False)
    termination_date = Column(Date)
    termination_type = Column(Text, CheckConstraint("termination_type IN ('Voluntary','NonRenewal','Lapse','Cancellation')"))
    reason = Column(Text)
    notes = Column(Text)

class BillingRecord(Base):
    __tablename__ = 'billing_records'
    
    billing_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    contact_id = Column(UUID, ForeignKey('contacts.contact_id'), nullable=False)
    policy_id = Column(UUID, ForeignKey('policies.policy_id'))
    billing_type = Column(Text)
    amount = Column(Numeric(12,2))
    billing_date = Column(Date)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class CommissionRule(Base):
    __tablename__ = 'commission_rules'
    
    rule_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    location_id = Column(UUID, ForeignKey('locations.location_id'), nullable=False)
    product_line = Column(Text)
    rate = Column(Numeric(5,4))
    effective_date = Column(Date)
    end_date = Column(Date)

class Commission(Base):
    __tablename__ = 'commissions'
    
    commission_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID, ForeignKey('policies.policy_id'))
    rule_id = Column(UUID, ForeignKey('commission_rules.rule_id'))
    amount = Column(Numeric(12,2))
    paid_date = Column(Date)

class Fee(Base):
    __tablename__ = 'fees'
    
    fee_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID, ForeignKey('policies.policy_id'))
    location_id = Column(UUID, ForeignKey('locations.location_id'))
    fee_type = Column(Text)
    amount = Column(Numeric(12,2))
    effective_date = Column(Date)

class Document(Base):
    __tablename__ = 'documents'
    
    document_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    entity_id = Column(UUID, nullable=False)
    entity_type = Column(Text, nullable=False)
    blob_info_id = Column(UUID)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class AcordForm(Base):
    __tablename__ = 'acord_forms'
    
    form_id = Column(UUID, primary_key=True, default=uuid.uuid4)
    api_form_id = Column(Integer)
    customer_id = Column(UUID, ForeignKey('contacts.contact_id'))
    policy_id = Column(UUID, ForeignKey('policies.policy_id'))
    template_id = Column(Integer)
    data = Column(JSONB)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now()) 