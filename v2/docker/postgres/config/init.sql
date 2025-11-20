---- init.sql
--CREATE TABLE IF NOT EXISTS customers (
--    customer_id SERIAL PRIMARY KEY,
--    first_name VARCHAR(100),
--    last_name VARCHAR(100),
--    email VARCHAR(255) UNIQUE,
--    phone VARCHAR(20),
--    created_at TIMESTAMP DEFAULT NOW()
--);
--
--INSERT INTO customers (first_name, last_name, email, phone)
--VALUES ('John', 'Doe', 'john.doe@example.com', '1234567890');
-- =============================================
-- Create Database
-- =============================================
--CREATE DATABASE simple_crm;
--\c simple_crm;

-- =============================================
-- TABLE: customers
-- Stores basic customer records used for identification, communication, and CRM operations.
-- This table acts as the foundation for all relationship tracking in the system.
-- =============================================
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    created_on DATE DEFAULT CURRENT_DATE
);

COMMENT ON TABLE customers IS
'Stores records for all customers in the CRM system. Includes identity fields, contact information, and account creation details.
This table is referenced by other modules such as sales and interactions, making it a central data entity.';

COMMENT ON COLUMN customers.customer_id IS
'Unique ID automatically assigned to each customer. This serves as the primary identifier in all related CRM workflows.';
COMMENT ON COLUMN customers.first_name IS
'Customer''s given name used for personalization and identification in communication and reporting.';
COMMENT ON COLUMN customers.last_name IS
'Family name of the customer. Used for matching, personalization, and formal documentation.';
COMMENT ON COLUMN customers.email IS
'Primary email address used for notifications, receipts, and identifying duplicate accounts.';
COMMENT ON COLUMN customers.created_on IS
'Date when the customer profile was created. Helps track account age, lifecycle stage, and churn metrics.';


-- =============================================
-- TABLE: employees
-- Contains records of staff responsible for sales, support, and customer interaction.
-- Used to map tasks, assign customers, and measure individual performance.
-- =============================================
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    department VARCHAR(100),
    hired_on DATE
);

COMMENT ON TABLE employees IS
'Stores information for all employees working across sales, support, and service functions.
Employee data is essential for assigning tasks, evaluating productivity, and linking customer-facing activities.';

COMMENT ON COLUMN employees.employee_id IS
'Unique identifier assigned to each employee. Used to track ownership of leads, sales, and interactions.';
COMMENT ON COLUMN employees.first_name IS
'Employee’s first name. Appears in reports, dashboards, and communication logs.';
COMMENT ON COLUMN employees.last_name IS
'Employee’s last name. Useful for hierarchical mapping and formal documentation.';
COMMENT ON COLUMN employees.department IS
'Department where the employee works (e.g., Sales, Support). Helps categorize workload and team structure.';
COMMENT ON COLUMN employees.hired_on IS
'Date when the employee joined the company. Important for tenure analytics and access provisioning.';


-- =============================================
-- TABLE: leads
-- Contains unqualified prospects entering the CRM pipeline through campaigns or inquiries.
-- Helps track the initial stages of the sales funnel before conversion to customers.
-- =============================================
CREATE TABLE leads (
    lead_id SERIAL PRIMARY KEY,
    lead_name VARCHAR(150),
    email VARCHAR(255),
    source VARCHAR(100),
    created_on DATE DEFAULT CURRENT_DATE
);

COMMENT ON TABLE leads IS
'Captures preliminary records of potential customers obtained from campaigns, referrals, or website forms.
This table supports lead management workflows and feeds the opportunity creation process.';

COMMENT ON COLUMN leads.lead_id IS
'Unique identifier automatically generated for each lead. Used in follow-ups, scoring, and pipeline tracking.';
COMMENT ON COLUMN leads.lead_name IS
'Name of the prospect or organization making an inquiry. Helps sales teams identify and qualify the lead.';
COMMENT ON COLUMN leads.email IS
'Email address provided by the lead. Used to initiate communication and detect duplicates.';
COMMENT ON COLUMN leads.source IS
'Indicates how the lead entered the system (e.g., Website, Event, Referral). Used for marketing attribution.';
COMMENT ON COLUMN leads.created_on IS
'Timestamp capturing when the lead was created. Helps with aging analysis and SLA enforcement.';


-- =============================================
-- TABLE: interactions
-- Logs communication events between employees and customers.
-- Supports customer service analysis, satisfaction tracking, and follow-up workflows.
-- =============================================
CREATE TABLE interactions (
    interaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    employee_id INT REFERENCES employees(employee_id),
    interaction_type VARCHAR(100),
    interaction_date DATE
);

COMMENT ON TABLE interactions IS
'Tracks every interaction that takes place between customers and company staff. This includes calls, emails, chats, or in-person meetings.
These logs help monitor engagement frequency, service quality, and customer satisfaction trends.';

COMMENT ON COLUMN interactions.interaction_id IS
'Unique identifier assigned to each interaction event. Essential for auditing and reference in CRM history.';
COMMENT ON COLUMN interactions.customer_id IS
'Customer involved in the interaction. Links back to the main customer profile for full engagement context.';
COMMENT ON COLUMN interactions.employee_id IS
'Employee who handled the interaction. Useful for performance evaluation and workflow ownership.';
COMMENT ON COLUMN interactions.interaction_type IS
'Classifies the type of interaction, such as Support, Inquiry, Complaint, or Follow-up. Helps group communication patterns.';
COMMENT ON COLUMN interactions.interaction_date IS
'Date on which the interaction occurred. Used for sequencing events and understanding customer timelines.';


-- =============================================
-- TABLE: sales
-- Represents completed sales transactions linked to customers and employees.
-- Provides the dataset needed for revenue reporting and performance analytics.
-- =============================================
CREATE TABLE sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    employee_id INT REFERENCES employees(employee_id),
    total_amount NUMERIC(10,2),
    sale_date DATE
);

COMMENT ON TABLE sales IS
'Stores all finalized sales transactions performed by employees for customers.
This data is central to revenue tracking, performance dashboards, and financial forecasting.';

COMMENT ON COLUMN sales.sale_id IS
'Unique ID representing each sale. Acts as the anchor for revenue and invoice mapping.';
COMMENT ON COLUMN sales.customer_id IS
'Customer who completed the purchase. Enables full purchase history tracking and customer value analytics.';
COMMENT ON COLUMN sales.employee_id IS
'Employee responsible for the sale. Provides attribution for performance ranking and incentives.';
COMMENT ON COLUMN sales.total_amount IS
'Total monetary value of the sale, including all purchased products or services. Used for revenue calculations.';
COMMENT ON COLUMN sales.sale_date IS
'Date when the sale was completed. Supports trend analysis, seasonality checks, and cohort reporting.';
