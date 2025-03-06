import aiomysql
import logging

logger = logging.getLogger(__name__)

class DBManager:
    
    def __init__(self, host, port, user, password, db_name):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.pool = None

    async def init_pool(self):
        """
        Инициализирует пул подключений к MariaDB.
        Заодно создаёт базу (если не существует),
        а затем внутри неё — таблицу open_interest_history (с уникальным индексом).
        """
        # Сначала подключимся к mysql-серверу без указания db
        try:
            logger.info(f"Подключаемся к MariaDB без указания базы ({self.host}:{self.port})...")
            conn = await aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            async with conn.cursor() as cur:
                await cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка при создании базы {self.db_name}: {e}")

        # Создаём пул
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db_name,
            autocommit=True,
            minsize=1,
            maxsize=5
        )

        # Создаём таблицу open_interest_history (если нет) с уникальным индексом
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS open_interest_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp BIGINT NOT NULL,
            sum_open_interest VARCHAR(50),
            sum_open_interest_value VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        unique_index_sql = """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_timestamp
        ON open_interest_history (symbol, timestamp);
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_table_sql)
                # В MariaDB 10.6 синтаксис IF NOT EXISTS для CREATE INDEX может не работать,
                # можно обойтись TRY/EXCEPT или вручную проверить.
                try:
                    await cur.execute(unique_index_sql)
                except Exception as e:
                    logger.warning(f"Не удалось создать уникальный индекс (возможно, уже существует): {e}")
        logger.info("Таблица open_interest_history и индекс проверены/созданы")

    async def close_pool(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def save_open_interest(self, symbol: str, oi_data: list):
        """
        Сохраняем список OI записей. Используем INSERT IGNORE + уникальный индекс
        (symbol, timestamp), чтобы не плодить дубликаты.
        """
        if not oi_data:
            return

        insert_sql = """
            INSERT IGNORE INTO open_interest_history
            (symbol, timestamp, sum_open_interest, sum_open_interest_value)
            VALUES (%s, %s, %s, %s)
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for oi in oi_data:
                    ts = oi.get("timestamp", 0)
                    sum_oi = oi.get("sumOpenInterest", "")
                    sum_oi_val = oi.get("sumOpenInterestValue", "")
                    try:
                        await cur.execute(insert_sql, (symbol, ts, sum_oi, sum_oi_val))
                    except Exception as e:
                        logger.error(
                            f"Ошибка при вставке OI (symbol={symbol}, ts={ts}): {e}"
                        )
