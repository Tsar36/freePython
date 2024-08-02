import asyncio
import asyncpg

async def anonymize_table(conn, tablename):
    async with conn.transaction():
        result = await conn.fetchval('SELECT anon.anonymize_table($1)', tablename)
        return result

async def main():
    # Подключение к базе данных
    conn = await asyncpg.connect(
        user='your_user',
        password='your_password',
        database='your_database',
        host='your_host',
        port='your_port'
    )

    # Список таблиц для анонимизации
    tables = ['table1', 'table2', 'table3', 'table4']

    # Создание асинхронных задач для анонимизации таблиц
    tasks = [anonymize_table(conn, table) for table in tables]

    # Запуск задач и ожидание их завершения
    results = await asyncio.gather(*tasks)

    # Вывод результатов
    for table, result in zip(tables, results):
        print(f'Table: {table}, Result: {result}')

    # Закрытие подключения
    await conn.close()

# Запуск главной асинхронной функции
asyncio.run(main())


'''
This example performs the following steps:

  1. Establishes a connection to the PostgreSQL database.
  2. Creates a list of tables that need to be anonymized.
  3. Creates asynchronous tasks for each table.
  4. Starts tasks and waits for them to complete.
  5. Displays results for each table.
  6. Closes the connection to the database.

Replace your_user, your_password, your_database, your_host and your_port with the appropriate values ​​for your PostgreSQL database.

This approach allows you to perform asynchronous database queries across multiple threads using the capabilities of asyncio and asyncpg.

'''