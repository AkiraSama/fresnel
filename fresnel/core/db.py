import logging

import aiopg
from discord.ext.commands import Bot
from pypika import PostgreSQLQuery

from fresnel import constants


log = logging.getLogger(__name__)


class DBManager:
    def __init__(self, bot: Bot):
        self.bot = bot

    async def _init(self):
        self.db_info = self.bot._config.get(
            'psql_info',
            default=constants.PSQL_DEFAULT_DICT,
            comment="PostgreSQL database and user information",
        )

        self.db_info = {
            k: v for k, v in self.db_info.items() if v is not None
        }

        needed = constants.PSQL_DEFAULT_DICT.keys() - self.db_info

        if needed:
            log.error("please fully configure your PostgreSQL information in "
                      "your configuration file. "
                      f"missing keys: {','.join(needed)}")
            raise ValueError(
                "Postgres config. "
                f"missing keys: {','.join(needed)}"
            )

        dsn = constants.PSQL_INFO_STR.format(**self.db_info)

        self.bot._db_pool = await aiopg.create_pool(dsn)
        log.info("db connection established")

        self.bot._db_Query = PostgreSQLQuery


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
