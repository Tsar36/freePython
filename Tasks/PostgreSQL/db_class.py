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
    def __init__(self, host, password):
        # Инициализация параметров подключения к базе данных
        self.host = host
        self.password = password

    async def anonymize_table(self, table_name: str, db_name: str):
        # Асинхронное подключение к базе данных
        conn = await asyncpg.connect(
            user='your_user',
            password=self.password,
            database=db_name,
            host=self.host,
            port='your_port'
        )
        # Логирование запуска анонимизации таблицы
        logging.info(f"Running `anon.anonymize_table()` on {table_name} in {db_name}")
        # Выполнение SQL-запроса для анонимизации таблицы
        result = await conn.fetchval('SELECT anon.anonymize_table($1)', table_name)
        # Закрытие соединения с базой данных
        await conn.close()
        # Возвращение результата выполнения запроса
        return result

    async def anonymize_tables(self, table_info: list):
        # Получение текущего асинхронного цикла событий
        loop = asyncio.get_running_loop()
        # Создание пула процессов с максимальным числом рабочих процессов, равным 4
        with ProcessPoolExecutor(max_workers=4) as executor:
            # Создание задач для анонимизации таблиц в пуле процессов
            futures = [
                loop.run_in_executor(executor, self.anonymize_table, table, db)
                for db, table in table_info
            ]
            # Ожидание завершения всех задач и сбор их результатов
            results = await asyncio.gather(*futures)
        # Возвращение списка результатов
        return results

    def get_dbs(self):
        # Метод для получения списка баз данных (реализация требуется)
        pass

    def get_db_cursor(self, db):
        # Метод для получения курсора базы данных (реализация требуется)
        pass

    def install_anonymizer(self, db, cursor):
        # Метод для установки анонимизатора в базе данных (реализация требуется)
        pass

    async def anonymize_database(self, host):
        # Создание экземпляра PSQL с параметрами подключения
        psql = PSQL(host, self.aws_secrets.get_secret_value(SecretId='db-pass-prod')['SecretString'])
        # Получение списка баз данных
        databases = psql.get_dbs()

        # Список для хранения информации о таблицах
        table_info = []
        for db in databases:
            # Получение курсора для базы данных
            with psql.get_db_cursor(db) as cursor:
                # Установка анонимизатора в базе данных
                psql.install_anonymizer(db, cursor)

                # Получение списка таблиц для текущей базы данных
                tables = get_tables_for_db(db)
                for table in tables:
                    # Добавление информации о таблицах в список
                    table_info.append((db, table))

        # Анонимизация таблиц параллельно, используя 4 процесса
        await psql.anonymize_tables(table_info)


# Функция для получения списка таблиц в базе данных (реализация требуется)
def get_tables_for_db(db):
    pass


# Запуск асинхронной функции
asyncio.run(anonymize_database(self, 'your_host'))

def get_tables_for_db(db):
    # Implement the method to get table names for the given database
    pass


async def anonymize_database(self, host):
    psql = PSQL(host, self.aws_secrets.get_secret_value(SecretId='db-pass-prod')['SecretString'])
    databases = psql.get_dbs()

    table_info = []
    for db in databases:
        with psql.get_db_cursor(db) as cursor:
            psql.install_anonymizer(db, cursor)

            tables = get_tables_for_db(db)
            for table in tables:
                table_info.append((db, table))

    await psql.anonymize_tables(table_info)


# To run the async function
asyncio.run(anonymize_database(self, 'your_host'))