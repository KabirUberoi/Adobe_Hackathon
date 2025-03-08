# Import necessary libraries
import json
import requests
import time
import sqlparse
import psycopg2

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "1103",
    "host": "localhost",
    "port": "5432"
}

conn = psycopg2.connect(**DB_CONFIG)

# Global variable to keep track of the total number of tokens
total_tokens = 0

def load_input_file(file_path):
    """
    Load input file which is a list of dictionaries.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def get_schema(conn):
    """
    Fetches a concise schema with table names and column names.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            ORDER BY table_name, ordinal_position;
        """)
        
        schema = {}
        for table, col in cur.fetchall():
            schema.setdefault(table, []).append(col)
    
    # Concise representation: Table -> List of column names
    return "\n".join(f"{table}: {', '.join(cols)}" for table, cols in schema.items())

def generate_sqls(data):
    """
    Generate SQL statements with single-line formatting.
    """
    sql_statements = []
    schema_info = get_schema(conn)
    
    system_prompt = {
        "role": "system",
        "content": f"""You are a SQL expert. Convert natural language to optimized SQL using this schema: {json.dumps(schema_info, indent=0)}. Single space between keywords Include semicolon Use explicit JOIN syntax and no explaination"""
    }
        
    print(system_prompt["content"])
    
    for nl_query in data:
        try:
            user_prompt = {
                "role": "user",
                "content": f"Convert to SQL: {nl_query}"
            }
            
            raw_sql = call_groq_api([system_prompt, user_prompt])
            
            # Clean and format
            cleaned_sql = raw_sql \
                .replace("```SQL", "") \
                .replace("```sql", "") \
                .replace("```", "") \
                .replace("\n", " ")  # Remove newlines
                
            # Standardize spacing and syntax
            cleaned_sql = " ".join(cleaned_sql.split()) \
                .replace(" ;", ";") \
                .replace("SELECT", "SELECT ") \
                .replace("FROM", " FROM ") \
                .strip()
                
            # Ensure ending semicolon
            if not cleaned_sql.endswith(';'):
                cleaned_sql += ';'
                
            sql_statements.append({
                "NL": nl_query,
                "Query": cleaned_sql
            })
            
            
        except Exception as e:
            print(f"Error processing query: {e}")
            sql_statements.append({
                "NL": nl_query,
                "Query": ""
            })
    
    return sql_statements

def correct_sqls(sql_statements):
    """
    Corrects SQL statements while ensuring single-line output.
    """
    corrected_sqls = []
    schema_info = get_schema(conn)
    system_prompt = {
        "role": "system",
        "content": f"You are a SQL query corrector. Schema is as follows: {json.dumps(schema_info, indent=1)}. NO newline, markdown, explanations, Single space between keywords"
    }
    
    print(system_prompt["content"])
    
    for entry in sql_statements:
        try:
            incorrect_sql = entry.get("IncorrectQuery", "").strip()
            nl_context = entry.get("NL", "").strip()

            if not incorrect_sql:
                corrected_sqls.append({"IncorrectQuery": "", "CorrectQuery": ""})
                continue

            # Construct prompt (explicitly forbid newlines)
            prompt = {"role": "user", "content": f"Original Intent: {nl_context or 'Not specified'}\n\n" f"Incorrect SQL: {incorrect_sql}\n\n"}
            
            raw_correction = call_groq_api([system_prompt, prompt])
            
            # Clean up output
            cleaned_sql = raw_correction \
                .replace("⁠  SQL", "") \
                .replace("  ⁠sql", "") \
                .replace("```", "") \
                .replace("\n", " ")  # Remove all newlines
                
            # Collapse multiple spaces and standardize
            cleaned_sql = " ".join(cleaned_sql.split()) \
                .replace(" ;", ";") \
                .strip()
                
            # Ensure ending semicolon
            if not cleaned_sql.endswith(';'):
                cleaned_sql += ';'

            corrected_sqls.append({
                "IncorrectQuery": incorrect_sql,
                "CorrectQuery": cleaned_sql
            })
            
        except Exception as e:
            print(f"Error: {e}")
            corrected_sqls.append({"IncorrectQuery": incorrect_sql, "CorrectQuery": ""})
    
    return corrected_sqls

def call_groq_api(messages, temperature=0.0, max_tokens=1000, n=1):
    """
    Call the Groq API to get a response from the language model, ensuring rate limits are respected.
    """
    global total_tokens
    url = "https://api.groq.com/openai/v1/chat/completions"
    
    api_key = "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"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    data = {
        "model": "llama-3.2-3b-preview",
        "messages": messages,
        'temperature': temperature,
        'max_tokens': max_tokens,
        'n': n
    }
    
    while True:  # Keep retrying if needed
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 429:  # Rate limit exceeded
            retry_after = int(response.headers.get("retry-after", 5))  # Default to 5 seconds if missing
            print(f"Rate limit hit. Waiting for {retry_after} seconds...")
            time.sleep(retry_after)
            continue  # Retry request
        
        response_json = response.json()
        
        if "choices" not in response_json:
            print("Error: API response missing 'choices' key.")
            print("Full response:", response_json)
            return "ERROR: Invalid API response"
        
        # Extract rate limit info
        remaining_requests = int(response.headers.get("x-ratelimit-remaining-requests", 1))
        remaining_tokens = int(response.headers.get("x-ratelimit-remaining-tokens", 1))
        reset_requests_time = float(response.headers.get("x-ratelimit-reset-requests", 60).replace("s", ""))  # Convert to float seconds
        reset_tokens_time = float(response.headers.get("x-ratelimit-reset-tokens", 60).replace("s", ""))
        
        # Check if we are close to exceeding limits
        if remaining_tokens < max_tokens:
            print(f"Low on tokens. Waiting {reset_tokens_time} seconds for token reset...")
            time.sleep(reset_tokens_time)
        
        if remaining_requests < 2:
            print(f"Low on requests. Waiting {reset_requests_time} seconds for request reset...")
            time.sleep(reset_requests_time)
        
        total_tokens += response_json.get('usage', {}).get('completion_tokens', 0)
        return response_json["choices"][0]["message"]["content"].strip()

def main():
    input_file_path_1 = 'train_generate_task.json'
    input_file_path_2 = 'train_query_correction_task.json'
    
    # Load data from input file
    data_1 = load_input_file(input_file_path_1)
    data_2 = load_input_file(input_file_path_2)
    
    # Extract NL queries
    nl_queries = [item["NL"] for item in data_1]
    
    start = time.time()
    # Generate SQL statements
    sql_statements = generate_sqls(nl_queries)
    generate_sqls_time = time.time() - start
    
    start = time.time()
    # Correct SQL statements
    corrected_sqls = correct_sqls(data_2)
    correct_sqls_time = time.time() - start
    
    assert len(data_2) == len(corrected_sqls) # If no answer, leave blank
    assert len(nl_queries) == len(sql_statements) # If no answer, leave blank
    
    # Get the outputs as a list of dicts with keys 'IncorrectQuery' and 'CorrectQuery'
    with open('output_sql_correction_task.json', 'w') as f:
        json.dump(corrected_sqls, f)    
    
    # Get the outputs as a list of dicts with keys 'NL' and 'Query'
    with open('output_sql_generation_task.json', 'w') as f:
        json.dump(sql_statements, f)
    
    return generate_sqls_time, correct_sqls_time

if __name__ == "__main__":
    generate_sqls_time, correct_sqls_time = main()
    print(f"Time taken to generate SQLs: {generate_sqls_time} seconds")
    print(f"Time taken to correct SQLs: {correct_sqls_time} seconds")
    print(f"Total tokens: {total_tokens}")
