from sqlalchemy import Column, String, DateTime, Integer, JSON, text
from sqlalchemy.ext.declarative import declarative_base

from models.base import RawDataModel

class RawContact(RawDataModel):
    """Raw contacts data from QQCatalyst API"""
    __tablename__ = "raw_contacts"

class RawPolicy(RawDataModel):
    """Raw policies data from QQCatalyst API"""
    __tablename__ = "raw_policies"

class RawQuote(RawDataModel):
    """Raw quotes data from QQCatalyst API"""
    __tablename__ = "raw_quotes"

class RawClaim(RawDataModel):
    """Raw claims data from QQCatalyst API"""
    __tablename__ = "raw_claims"

class RawDocument(RawDataModel):
    """Raw documents data from QQCatalyst API"""
    __tablename__ = "raw_documents"

class RawApplication(RawDataModel):
    """Raw applications data from QQCatalyst API"""
    __tablename__ = "raw_applications"

class RawRenewal(RawDataModel):
    """Raw renewals data from QQCatalyst API"""
    __tablename__ = "raw_renewals"

class RawTermination(RawDataModel):
    """Raw terminations data from QQCatalyst API"""
    __tablename__ = "raw_terminations"

class RawBillingRecord(RawDataModel):
    """Raw billing records data from QQCatalyst API"""
    __tablename__ = "raw_billing_records"

class RawCommissionRule(RawDataModel):
    """Raw commission rules data from QQCatalyst API"""
    __tablename__ = "raw_commission_rules"

class RawCommission(RawDataModel):
    """Raw commissions data from QQCatalyst API"""
    __tablename__ = "raw_commissions"

class RawFee(RawDataModel):
    """Raw fees data from QQCatalyst API"""
    __tablename__ = "raw_fees"

class RawAcordForm(RawDataModel):
    """Raw ACORD forms data from QQCatalyst API"""
    __tablename__ = "raw_acord_forms"

class RawTask(RawDataModel):
    """Raw tasks data from QQCatalyst API"""
    __tablename__ = "raw_tasks"

class RawTextMessage(RawDataModel):
    """Raw text messages data from QQCatalyst API"""
    __tablename__ = "raw_text_messages"

class RawCarrier(RawDataModel):
    """Raw carriers data from QQCatalyst API"""
    __tablename__ = "raw_carriers"

class RawCommercialAutoDriver(RawDataModel):
    """Raw commercial auto drivers data from QQCatalyst API"""
    __tablename__ = "raw_commercial_auto_drivers"

class RawCommercialAutoVehicle(RawDataModel):
    """Raw commercial auto vehicles data from QQCatalyst API"""
    __tablename__ = "raw_commercial_auto_vehicles"

class RawEmployee(RawDataModel):
    """Raw employees data from QQCatalyst API"""
    __tablename__ = "raw_employees"

class RawLocation(RawDataModel):
    """Raw locations data from QQCatalyst API"""
    __tablename__ = "raw_locations"

class RawNote(RawDataModel):
    """Raw notes data from QQCatalyst API"""
    __tablename__ = "raw_notes"

class RawSignature(RawDataModel):
    """Raw signatures data from QQCatalyst API"""
    __tablename__ = "raw_signatures"

class RawSocialMedia(RawDataModel):
    """Raw social media data from QQCatalyst API"""
    __tablename__ = "raw_social_media"

class RawWebsite(RawDataModel):
    """Raw websites data from QQCatalyst API"""
    __tablename__ = "raw_websites"

class RawUnderwritingQuestion(RawDataModel):
    """Raw underwriting questions data from QQCatalyst API"""
    __tablename__ = "raw_underwriting_questions"

# New models for additional endpoints
class RawAddressInfo(RawDataModel):
    """Raw address info data from QQCatalyst API"""
    __tablename__ = "raw_address_info"

class RawBusinessLogic(RawDataModel):
    """Raw business logic data from QQCatalyst API"""
    __tablename__ = "raw_business_logic"

class RawCampaignTemplate(RawDataModel):
    """Raw campaign template data from QQCatalyst API"""
    __tablename__ = "raw_campaign_templates"

class RawCancellationReason(RawDataModel):
    """Raw cancellation reasons data from QQCatalyst API"""
    __tablename__ = "raw_cancellation_reasons"

class RawStatistics(RawDataModel):
    """Raw statistics data from QQCatalyst API"""
    __tablename__ = "raw_statistics"

class RawSubline(RawDataModel):
    """Raw sublines data from QQCatalyst API"""
    __tablename__ = "raw_sublines"

class RawTransactionConcept(RawDataModel):
    """Raw transaction concepts data from QQCatalyst API"""
    __tablename__ = "raw_transaction_concepts"

class RawTransformationProduct(RawDataModel):
    """Raw transformation products data from QQCatalyst API"""
    __tablename__ = "raw_transformation_products"

class RawTransformationSetup(RawDataModel):
    """Raw transformation setup data from QQCatalyst API"""
    __tablename__ = "raw_transformation_setup"

class RawTwilioSettings(RawDataModel):
    """Raw Twilio settings data from QQCatalyst API"""
    __tablename__ = "raw_twilio_settings"

class RawUserInfo(RawDataModel):
    """Raw user info data from QQCatalyst API"""
    __tablename__ = "raw_user_info"

class RawUserPreference(RawDataModel):
    """Raw user preferences data from QQCatalyst API"""
    __tablename__ = "raw_user_preferences"

class RawUserSetting(RawDataModel):
    """Raw user settings data from QQCatalyst API"""
    __tablename__ = "raw_user_settings" 