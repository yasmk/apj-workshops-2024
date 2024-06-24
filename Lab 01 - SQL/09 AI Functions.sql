/*
 * Invokes an existing Databricks Model Serving endpoint and parses and returns its response.
 */

SELECT
  ai_query(
    'databricks-mixtral-8x7b-instruct',
    'What is Databricks?'
  );


/*
 * The ai_mask() function allows you to invoke a state-of-the-art 
 * generative AI model to mask specified entities in a given text using SQL.
 * This function uses a chat model serving endpoint made available by Databricks Foundation Model APIs.
 */

SELECT
  ai_mask(
    'John Doe lives in New York. His email is john.doe@example.com.',
    array('person', 'email')
  );


/*
 * The ai_extract() function allows you to invoke a state-of-the-art 
 * generative AI model to extract entities specified by labels from a given text using SQL.
 * This function uses a chat model serving endpoint made available by Databricks Foundation Model APIs.
 */

SELECT
  ai_extract(
    'John Doe lives in New York and works for Acme Corp.',
    array('person', 'location', 'organization')
  );