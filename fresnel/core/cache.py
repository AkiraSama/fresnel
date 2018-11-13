import asyncio
import logging

from discord import Guild, Role
from discord.ext.commands import Bot


log = logging.getLogger(__name__)


class CacheManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.bot.fresnel_cache_flag = asyncio.Event()
        self.bot.role_name_cache = {}

    async def _init(self):
        for guild in self.bot.guilds:
            self.bot.role_name_cache[guild.id] = {
                role.name.upper(): role.id for role in guild.roles
            }

        self.bot.fresnel_cache_flag.set()

    def __unload(self):
        self.bot.fresnel_cache_flag.clear()

    async def on_guild_join(self, guild: Guild):
        self.bot.role_name_cache[guild.id] = {
            role.name.upper(): role.id for role in guild.roles
        }

    async def on_guild_role_update(self, before: Role, after: Role):
        if before.name != after.name:
            self.bot.role_name_cache[
                before.guild.id
            ][
                after.name.upper()
            ] = before.id
            del self.bot.role_name_cache[before.guild.id][before.name.upper()]


async def _setup(bot: Bot):
    await bot.wait_until_ready()
    cog = CacheManager(bot)
    await cog._init()
    log.info("adding CacheManager cog")
    bot.add_cog(cog)


def setup(bot: Bot):
    log.info("scheduling cache setup")
    bot.loop.create_task(_setup(bot))


def teardown(bot: Bot):
    log.info("removing CacheManager cog")
    bot.remove_cog(CacheManager.__name__)
