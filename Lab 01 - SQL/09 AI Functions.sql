-- Ask an LLM
SELECT
  ai_query(
    'databricks-mixtral-8x7b-instruct',
    'What is Databricks?'
  );


-- Mask values
SELECT
  ai_mask(
    'John Doe lives in New York. His email is john.doe@example.com.',
    array('person', 'email')
  );


-- Extract values
SELECT
  ai_extract(
    'John Doe lives in New York and works for Acme Corp.',
    array('person', 'location', 'organization')
  );