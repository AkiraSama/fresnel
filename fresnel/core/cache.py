import asyncio
import logging
import re
import shlex

from discord import Guild, Role
from discord.ext.commands import BadArgument, Bot, Context, NoPrivateMessage


log = logging.getLogger(__name__)

ID_MATCH = re.compile(r'([0-9]{15,21})$')
ROLE_ID_MATCH = re.compile(r'<@&([0-9]+)>$')


class CacheManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.bot.fresnel_cache_flag = asyncio.Event()
        self.role_name_cache = {}

        self.bot.convert_roles = self.convert_roles

    async def _init(self):
        for guild in self.bot.guilds:
            self.role_name_cache[guild.id] = {
                role.name.upper(): role.id for role in guild.roles
            }

        self.bot.fresnel_cache_flag.set()

    def __unload(self):
        self.bot.fresnel_cache_flag.clear()

    def convert_roles(self, ctx: Context, full_message: str):
        guild = ctx.message.guild
        if not guild:
            raise NoPrivateMessage()

        args = shlex.split(full_message)

        results = []
        try:
            for arg in args:
                match = ID_MATCH.match(arg) or ROLE_ID_MATCH.match(arg)
                if match:
                    result = guild.get_role(int(match.group(1)))
                else:
                    result = guild.get_role(
                        self.role_name_cache[guild.id].get(arg.upper())
                    )

                if result is None:
                    raise BadArgument(f'Role "{arg}" not found.')
                results.append(result)
        except BadArgument:
            if not results:
                result = guild.get_role(
                    self.role_name_cache[guild.id].get(full_message.upper())
                )

                if result is None:
                    raise
                results.append(result)
            else:
                raise

        return results

    async def on_guild_join(self, guild: Guild):
        self.role_name_cache[guild.id] = {
            role.name.upper(): role.id for role in guild.roles
        }

    async def on_guild_role_update(self, before: Role, after: Role):
        if before.name != after.name:
            self.role_name_cache[
                before.guild.id
            ][
                after.name.upper()
            ] = before.id
            try:
                del self.role_name_cache[before.guild.id][before.name.upper()]
            except KeyError:
                pass

    async def on_guild_role_create(self, role: Role):
        self.role_name_cache[role.guild.id][role.name.upper()] = role.id


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
