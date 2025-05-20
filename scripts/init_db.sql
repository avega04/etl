-- Function to create database if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'qqcatalyst_etl') THEN
        PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE qqcatalyst_etl');
    END IF;
END
$$;

\c qqcatalyst_etl;

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Agency & Locations
CREATE TABLE IF NOT EXISTS agencies (
  agency_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name        TEXT       NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS locations (
  location_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  agency_id   UUID       NOT NULL REFERENCES agencies(agency_id),
  name        TEXT,
  address_id  UUID,  -- FK to centralized addresses table if used
  phone       TEXT,
  created_at  TIMESTAMPTZ DEFAULT now()
);

-- 2. Employees & Departments
CREATE TABLE IF NOT EXISTS employees (
  employee_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  location_id UUID       NOT NULL REFERENCES locations(location_id),
  first_name  TEXT       NOT NULL,
  last_name   TEXT       NOT NULL,
  email       TEXT       UNIQUE,
  role        TEXT,
  status      TEXT,      -- e.g. 'active','inactive'
  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS departments (
  department_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  location_id   UUID       NOT NULL REFERENCES locations(location_id),
  name          TEXT       NOT NULL
);

-- 3. Contacts & Communication
CREATE TABLE IF NOT EXISTS contacts (
  contact_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  type         TEXT CHECK (type IN ('Individual','Business')) NOT NULL,
  first_name   TEXT,
  last_name    TEXT,
  company_name TEXT,
  created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS contact_addresses (
  address_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  contact_id UUID       NOT NULL REFERENCES contacts(contact_id),
  type       TEXT,      -- e.g. 'home','mailing','office'
  line1      TEXT,
  city       TEXT,
  state      TEXT,
  zip        TEXT,
  country    TEXT
);

CREATE TABLE IF NOT EXISTS contact_emails (
  email_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  contact_id UUID       NOT NULL REFERENCES contacts(contact_id),
  email      TEXT       NOT NULL,
  is_primary BOOLEAN   DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS contact_phones (
  phone_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  contact_id UUID       NOT NULL REFERENCES contacts(contact_id),
  number     TEXT       NOT NULL,
  type       TEXT,      -- e.g. 'mobile','home','work'
  is_primary BOOLEAN   DEFAULT FALSE
);

-- 4. Carriers
CREATE TABLE IF NOT EXISTS carriers (
  carrier_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name       TEXT       NOT NULL,
  parent_id  UUID       REFERENCES carriers(carrier_id),
  rating     TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 5. Quotes & Applications
CREATE TABLE IF NOT EXISTS quotes (
  quote_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  contact_id  UUID       NOT NULL REFERENCES contacts(contact_id),
  location_id UUID       NOT NULL REFERENCES locations(location_id),
  carrier_id  UUID       NOT NULL REFERENCES carriers(carrier_id),
  lob_type    TEXT       NOT NULL,         -- e.g. 'Auto','Home'
  quote_date  TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_until TIMESTAMPTZ,
  status      TEXT       NOT NULL CHECK (status IN ('Draft','Issued','Expired','Revised')),
  quote_data  JSONB,
  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quotes_validity ON quotes(valid_until);

CREATE TABLE IF NOT EXISTS applications (
  application_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  quote_id         UUID       NOT NULL REFERENCES quotes(quote_id),
  contact_id       UUID       NOT NULL REFERENCES contacts(contact_id),
  submitted_at     TIMESTAMPTZ DEFAULT now(),
  status           TEXT       NOT NULL CHECK (status IN ('Pending','Underwriting','Approved','Declined')),
  application_data JSONB,
  created_at       TIMESTAMPTZ DEFAULT now()
);

-- 6. Policies & Policy Details
CREATE TABLE IF NOT EXISTS policies (
  policy_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  policy_number   TEXT UNIQUE NOT NULL,
  contact_id      UUID       NOT NULL REFERENCES contacts(contact_id),
  location_id     UUID       NOT NULL REFERENCES locations(location_id),
  carrier_id      UUID       NOT NULL REFERENCES carriers(carrier_id),
  line_of_business TEXT,
  status          TEXT       NOT NULL CHECK (
                     status IN (
                       'Quoted','Applied','Bound','Active','Expired','Terminated'
                     )
                   ),
  effective_date  DATE,
  expiration_date DATE,
  total_premium   NUMERIC(12,2),
  created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_policies_effective ON policies(effective_date);

CREATE TABLE IF NOT EXISTS policy_details (
  detail_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  policy_id   UUID       NOT NULL REFERENCES policies(policy_id),
  lob_type    TEXT       NOT NULL,  -- e.g. 'CommercialAuto','InlandMarine'
  detail_data JSONB,
  created_at  TIMESTAMPTZ DEFAULT now()
);

-- Sub-entities under policy_details
CREATE TABLE IF NOT EXISTS drivers (
  driver_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  detail_id    UUID       NOT NULL REFERENCES policy_details(detail_id),
  name         TEXT,
  license_number TEXT,
  dob          DATE
);

CREATE TABLE IF NOT EXISTS vehicles (
  vehicle_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  detail_id  UUID       NOT NULL REFERENCES policy_details(detail_id),
  vin        TEXT,
  make       TEXT,
  model      TEXT,
  year       INT
);

CREATE TABLE IF NOT EXISTS coverages (
  coverage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  detail_id   UUID       NOT NULL REFERENCES policy_details(detail_id),
  code        TEXT,
  limit_amount NUMERIC(12,2),
  deductible   NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS equipment (
  equipment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  detail_id    UUID       NOT NULL REFERENCES policy_details(detail_id),
  description  TEXT,
  value        NUMERIC(12,2)
);

-- 7. Claims, Renewals & Terminations
CREATE TABLE IF NOT EXISTS claims (
  claim_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  policy_id     UUID       NOT NULL REFERENCES policies(policy_id),
  contact_id    UUID,      -- claimant (can differ from policyholder)
  claim_number  TEXT UNIQUE,
  reported_date DATE,
  status        TEXT NOT NULL CHECK (
                   status IN ('Open','Investigating','Reserved','Paid','Closed')
                 ),
  claim_data    JSONB,
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_claims_policy ON claims(policy_id);

CREATE TABLE IF NOT EXISTS renewals (
  renewal_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  policy_id       UUID       NOT NULL REFERENCES policies(policy_id),
  offer_date      DATE       NOT NULL,
  new_effective   DATE,
  new_expiration  DATE,
  premium_offered NUMERIC(12,2),
  status          TEXT       NOT NULL CHECK (status IN ('Offered','Accepted','Declined')),
  renewal_data    JSONB,
  created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_renewals_policy ON renewals(policy_id);

CREATE TABLE IF NOT EXISTS terminations (
  termination_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  policy_id        UUID       NOT NULL REFERENCES policies(policy_id),
  termination_date DATE,
  termination_type TEXT CHECK (
                     termination_type IN (
                       'Voluntary','NonRenewal','Lapse','Cancellation'
                     )
                   ),
  reason           TEXT,
  notes            TEXT
);

-- 8. Billing, Commissions & Fees
CREATE TABLE IF NOT EXISTS billing_records (
  billing_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  contact_id   UUID       NOT NULL REFERENCES contacts(contact_id),
  policy_id    UUID       REFERENCES policies(policy_id),
  billing_type TEXT,      -- e.g. 'CompanyPremium','Customer'
  amount       NUMERIC(12,2),
  billing_date DATE,
  created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS commission_rules (
  rule_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  location_id   UUID       NOT NULL REFERENCES locations(location_id),
  product_line  TEXT,
  rate          NUMERIC(5,4),
  effective_date DATE,
  end_date      DATE
);

CREATE TABLE IF NOT EXISTS commissions (
  commission_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  policy_id     UUID       REFERENCES policies(policy_id),
  rule_id       UUID       REFERENCES commission_rules(rule_id),
  amount        NUMERIC(12,2),
  paid_date     DATE
);

CREATE TABLE IF NOT EXISTS fees (
  fee_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  policy_id   UUID       REFERENCES policies(policy_id),
  location_id UUID       REFERENCES locations(location_id),
  fee_type    TEXT,
  amount      NUMERIC(12,2),
  effective_date DATE
);

-- 9. Documents & ACORD Forms
CREATE TABLE IF NOT EXISTS documents (
  document_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  entity_id    UUID       NOT NULL,    -- contact_id, policy_id, etc.
  entity_type  TEXT       NOT NULL,    -- 'Contact','Policy', etc.
  blob_info_id UUID,
  created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS acord_forms (
  form_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  api_form_id   INT,                    -- original QQCatalyst form ID
  customer_id   UUID       REFERENCES contacts(contact_id),
  policy_id     UUID       REFERENCES policies(policy_id),
  template_id   INT,
  data          JSONB,
  description   TEXT,
  created_at    TIMESTAMPTZ DEFAULT now()
); 