import logging

import aiopg
import aioredis
from discord.ext.commands import Bot, Cog
from pypika import PostgreSQLQuery

from fresnel import constants


log = logging.getLogger(__name__)


class DBManager(Cog):
    REDIS_DEFAULT_DICT = {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'password': None,
    }

    def __init__(self, bot: Bot):
        self.bot = bot

    async def _init(self):
        self.db_info = self.bot._config.get(
            'psql_info',
            default=constants.PSQL_DEFAULT_DICT,
            comment="PostgreSQL database and user information",
        )

        self.redis_info = self.bot._config.get(
            'redis_info',
            default=self.REDIS_DEFAULT_DICT,
            comment="Redis remote dictionary server connection information",
        )

        self.db_info = {
            k: v for k, v in self.db_info.items() if v is not None
        }

        needed = constants.PSQL_DEFAULT_DICT.keys() - self.db_info

        if needed:
            log.error("please fully configure your PostgreSQL information in "
                      "your configuration file. "
                      f"missing keys: {', '.join(needed)}")
            raise ValueError(
                "Postgres config. "
                f"missing keys: {', '.join(needed)}"
            )

        needed = {'host', 'port', 'db'} - self.redis_info.keys()

        if needed:
            log.error("please fully configure your Redis information in "
                      "your configuration file. "
                      f"missing keys: {', '.join(needed)}")
            raise ValueError(
                "Redis config. "
                f"missing keys: {', '.join(needed)}"
            )

        dsn = constants.PSQL_INFO_STR.format(**self.db_info)

        self.bot._db_pool = await aiopg.create_pool(dsn)
        log.info("db connection established")

        self.bot._db_Query = PostgreSQLQuery

        self.bot.redis_pool = await aioredis.create_redis_pool(
            (self.redis_info['host'], self.redis_info['port']),
            db=self.redis_info['db'],
            password=self.redis_info.get('password'),
            encoding='utf-8',
        )
        log.info("Redis connection established")

    def __unload(self):
        self.bot.redis_pool.close()


async def _setup(bot: Bot):
    cog = DBManager(bot)
    await cog._init()
    log.info("adding DBManager cog")
    bot.add_cog(cog)


def setup(bot: Bot):
    log.info("running db setup until complete")
    bot.loop.run_until_complete(_setup(bot))


def teardown(bot: Bot):
    log.info("removing DBManager cog")
    bot.remove_cog(DBManager.__name__)
