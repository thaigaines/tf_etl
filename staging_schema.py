"""
DROP TABLE IF EXISTS
	staging.plan,
	staging.plan_benefit,
	staging.carrier,
	staging.procedure,
	staging.service_type,
	staging.plan_type,
	staging.plan_category,
	staging.network_type
CASCADE;

CREATE TABLE staging.carrier (
	carrier_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	"name" TEXT UNIQUE NOT NULL
);

CREATE TABLE staging.plan_type (
	plan_type_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	"name" TEXT UNIQUE NOT NULL
);

CREATE TABLE staging.network_type (
	network_type_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	"name" TEXT UNIQUE NOT NULL
);

CREATE TABLE staging.service_type (
	service_type_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	"type" TEXT UNIQUE NOT NULL
);

CREATE TABLE staging.plan_category (
	plan_category_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	category TEXT UNIQUE NOT NULL
);

CREATE TABLE staging.procedure (
	cdt_code TEXT UNIQUE NOT NULL PRIMARY KEY,
	"name" TEXT NOT NULL,
	service_type_id INT NOT NULL REFERENCES staging.service_type(service_type_id)
);

CREATE TABLE staging.plan (
    plan_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    carrier_id INT NOT NULL
		REFERENCES staging.carrier(carrier_id),
    plan_type_id INT NOT NULL
		REFERENCES staging.plan_type(plan_type_id),
    code TEXT UNIQUE NOT NULL,
    "name" TEXT NOT NULL,
    "cost" NUMERIC(10, 2),
    load_timestamp TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE staging.plan_benefit (
    plan_benefit_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plan_id INT NOT NULL
        REFERENCES staging.plan(plan_id)
        ON DELETE CASCADE,
    cdt_code TEXT NOT NULL
        REFERENCES staging.procedure(cdt_code),
    network_type_id INT NOT NULL
        REFERENCES staging.network_type(network_type_id),
    service_type_id INT NOT NULL
        REFERENCES staging.service_type(service_type_id),
    copay NUMERIC(10,2),
    coinsurance NUMERIC(10,2),
    annual_max_applies BOOLEAN,
    subject_to_deductible BOOLEAN,
    discount_percent NUMERIC(10,2),
    load_timestamp TIMESTAMPTZ DEFAULT now()
);
"""
