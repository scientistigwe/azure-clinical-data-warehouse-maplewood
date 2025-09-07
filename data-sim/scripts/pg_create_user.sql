-- Create user
CREATE USER super_user WITH PASSWORD 'super_user';

-- Grant read-only access to relevant tables
GRANT SELECT ON TABLE patients, encounters, labs TO super_user;

-- list tables
SELECT * FROM patients LIMIT 5;
