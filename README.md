### README for SQL Generation and Correction Script

#### Overview
This Python script automates the processes of generating and correcting SQL queries. It interfaces with an external API (Groq) to convert natural language queries into SQL and to correct provided SQL queries against a database schema. The script also handles API rate limiting and token management efficiently.

#### System Requirements
- Python 3.7 or higher
- `requests` library
- `json` library
- `psycopg2` library for PostgreSQL database connection
- A running PostgreSQL database

#### Setup Instructions
1. **Install Python Dependencies**: Ensure that Python 3.7 or above is installed on your system. Then install the required Python libraries by running:
   ```bash
   pip install requests psycopg2
   ```

2. **Database Configuration**: Modify the `DB_CONFIG` dictionary in the script to match the credentials of your PostgreSQL database:
   ```python
   DB_CONFIG = {
       "dbname": "your_database_name",
       "user": "your_username",
       "password": "your_password",
       "host": "localhost",
       "port": "5432"
   }
   ```

3. **API Key Configuration**: Replace `your_api_key_here` with your actual Groq API key:
   ```python
   api_key = "your_api_key_here"
   ```

#### Script Components
1. **Database Connection**: Establishes a connection to the PostgreSQL database and fetches schema details.
2. **Input File Loading**: Loads input data from JSON files containing natural language queries.
3. **SQL Generation**: Converts natural language queries into SQL statements using the Groq API.
4. **SQL Correction**: Corrects provided SQL queries using the Groq API, considering the database schema.
5. **Rate Limit Handling**: Manages API rate limits and token usage, ensuring smooth operation under API constraints.

#### Running the Script
1. Place your input JSON files in the same directory as the script. These should contain natural language queries and SQL statements for correction.
2. Execute the script from the command line:
   ```bash
   python script_name.py
   ```
   Replace `script_name.py` with the name of your script file.

3. The script will generate two output files:
   - `output_sql_generation_task.json`: Contains generated SQL queries from natural language inputs.
   - `output_sql_correction_task.json`: Contains corrected SQL queries.

#### Output Files
- **Output SQL Generation Task**: A JSON file where each entry corresponds to a natural language query and its respective generated SQL query.
- **Output SQL Correction Task**: A JSON file where each entry corresponds to an incorrect SQL query, the intended natural language description, and its corrected SQL version.

#### Note
Ensure that your API limits and database connection are properly configured to avoid interruptions during execution. The script prints time taken for each task and total tokens used, aiding in performance monitoring and API usage tracking.
