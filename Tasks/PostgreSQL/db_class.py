'''
The problem right now is that when you run SELECT anon.anonymize_database(), it blocks the Python script until it finishes.
The anonymize_database() function takes only 1 CPU, so in theory, we could run 4 at the same time.
However, let's not anonymize_database() for each database, but run anonymize_table().
So, if you do this, we could run 4 processes in parallel without limitation of databases.
For example, the transactions service has 10 tables to anonymize and the ML service has one.
Make it that way so it will always run 4 processes.
'''

import asyncio
import asyncpg
import logging
from concurrent.futures import ProcessPoolExecutor


class PSQL:

    async def anonymize_table(self, table_name: str):
        conn = await asyncpg.connect(
            user='your_user',
            password='your_password',
            database='your_database',
            host='your_host',
            port='your_port'
        )
        logging.info(f"Running `anon.anonymize_table()` on {table_name}")
        result = await conn.fetchval('SELECT anon.anonymize_table($1)', table_name)
        await conn.close()
        return result

    async def anonymize_db(self, tables: list):
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = [
                loop.run_in_executor(executor, self.anonymize_table, table)
                for table in tables
            ]
            results = await asyncio.gather(*futures)
        return results


# Example usage
async def main():
    psql = PSQL()

    transactions_service_tables = [
        'table1', 'table2', 'table3', 'table4',
        'table5', 'table6', 'table7', 'table8',
        'table9', 'table10'
    ]

    ml_service_tables = ['ml_table1']

    all_tables = transactions_service_tables + ml_service_tables

    results = await psql.anonymize_db(all_tables)

    for table, result in zip(all_tables, results):
        logging.info(f'Table: {table}, Result: {result}')


# Run the async function
asyncio.run(main())


'''
Explanation:

	1.	Anonymize Table Function:
	•	The anonymize_table function is modified to create and close a connection for each table anonymization task.
	2.	Parallel Execution with ProcessPoolExecutor:
	•	concurrent.futures.ProcessPoolExecutor is used to run the anonymization tasks in parallel. The max_workers=4 parameter ensures that no more than 4 tasks are run simultaneously.
	•	loop.run_in_executor is used to submit each task to the executor. This method runs the function in a separate process.
	3.	Main Function:
	•	The main function prepares the list of tables to be anonymized and calls the anonymize_db function.
	•	Results are logged for each table after the anonymization process is complete.

Running the Code:

Replace the database connection parameters (your_user, your_password, your_database, your_host, and your_port) with your actual database details.

This approach ensures that up to 4 anonymization tasks are run in parallel, making efficient use of CPU resources.
'''