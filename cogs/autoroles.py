import asyncio
import logging
import string
from functools import reduce
from operator import or_

from discord import Member, Message, Role
from discord.ext.commands import Bot, Context, group
from psycopg2 import IntegrityError
from pypika import Table


log = logging.getLogger(__name__)

AUTO_SCHEMA = '''
CREATE TABLE IF NOT EXISTS "{name}" (
    role_id BIGINT NOT NULL,
    thz BIGINT NOT NULL,
    PRIMARY KEY (role_id)
)
'''

THZ_SCHEMA = '''
CREATE TABLE IF NOT EXISTS "{name}" (
    user_id BIGINT NOT NULL,
    thz BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id)
)
'''

CHARS = frozenset(string.ascii_letters + string.punctuation)


class AutoRoles:
    THZ_INTERVAL = 120

    def __init__(self, bot: Bot):
        self.bot = bot
        self.pool = bot._db_pool
        self.Query = bot._db_Query
        self.tables = {}
        self.cache = {}
        self.time_cache = {}
        self.ptask = None

    async def _init(self):
        for guild in self.bot.guilds:
            auto_name = f'autoroles-{guild.id}'
            thz_name = f'thz-{guild.id}'

            self.tables[guild.id] = {}
            self.tables[guild.id]['auto'] = auto_table = Table(auto_name)
            self.tables[guild.id]['thz'] = thz_table = Table(thz_name)

            self.cache[guild.id] = {}
            self.cache[guild.id]['auto'] = {}
            self.cache[guild.id]['thz'] = {}

            self.time_cache[guild.id] = {}

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        THZ_SCHEMA.format(name=thz_name)
                    )

                    await cur.execute(str(
                        self.Query.from_(thz_table).select(
                            thz_table.user_id, thz_table.thz,
                        )
                    ))

                    cleanup = []
                    async for user_id, thz in cur:
                        user = guild.get_member(user_id)

                        if user:
                            self.cache[guild.id]['thz'][user.id] = thz
                        else:
                            cleanup.append(user_id)

                    if cleanup:
                        await self._remove_users(guild.id, *cleanup)

                async with conn.cursor() as cur:
                    await cur.execute(
                        AUTO_SCHEMA.format(name=auto_name)
                    )

                    await cur.execute(str(
                        self.Query.from_(auto_table).select(
                            auto_table.role_id, auto_table.thz,
                        )
                    ))

                    cleanup = []
                    async for role_id, thz in cur:
                        role = guild.get_role(role_id)

                        if role:
                            self.cache[guild.id]['auto'][role.id] = thz
                        else:
                            cleanup.append(role_id)

                    if cleanup:
                        await self._remove_roles(guild.id, *cleanup)

        await self.bot.fresnel_cache_flag.wait()

        self.ptask = self.bot.loop.create_task(
            self.periodic()
        )

    def __unload(self):
        self.ptask.cancel()

    async def periodic(self):
        while True:
            await asyncio.sleep(self.THZ_INTERVAL)
            try:
                log.info("allocating THz")
                await self._periodic()
            except Exception as e:
                log.error(e)

    async def _periodic(self):
        time_cache = self.time_cache
        self.time_cache = {guild.id: {} for guild in self.bot.guilds}

        for guild_id, users in time_cache.items():
            for user_id, delta in users.items():
                if delta is None:
                    inc = 1
                    continue
                inc = 1 + delta.get('len', 0) + delta.get('var', 0)
                self.cache[guild_id][
                    'thz'
                ][user_id] = inc + self.cache[guild_id]['thz'].get(user_id, 0)

        async with self.pool.acquire() as conn:
            for guild_id, users in time_cache.items():
                table = self.tables[guild_id]['thz']
                async with conn.cursor() as cur:
                    for user_id, delta in users.items():
                        try:
                            await cur.execute(str(
                                self.Query.into(
                                    table
                                ).insert(
                                    user_id,
                                    self.cache[guild_id]['thz'][user_id]
                                )
                            ))
                        except IntegrityError:
                            await cur.execute(str(
                                self.Query.update(table).set(
                                    table.thz,
                                    self.cache[guild_id]['thz'][user_id],
                                ).where(
                                    table.user_id == user_id
                                )
                            ))

    async def on_message(self, message: Message):
        if message.author.bot:
            return

        delta = self.time_cache[message.guild.id].get(message.author.id, {})

        if delta is None:
            return

        length = len(message.content)
        if length >= 50:
            delta['len'] = min(delta.get('len', 0) + 1, 2)

        var = set(message.content)
        if len(CHARS & var):
            delta['var'] = min(delta.get('var', 0) + 1, 3)

        latest = delta.get('latest', (length, var, 0))
        if latest[0] == length and latest[1] == var:
            delta['latest'] = (length, var, latest[2] + 1)

        if latest[2] >= 5:
            self.time_cache[message.guild.id][message.author.id] = None
        else:
            self.time_cache[message.guild.id][message.author.id] = delta

    async def _remove_users(self, guild_id, *user_ids):
        table = self.tables[guild_id]['thz']
        where = reduce(
            or_,
            (table.user_id == user_id for user_id in user_ids),
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(str(
                    self.Query.from_(table).where(where).delete()
                ))

        for user_id in user_ids:
            self.cache[guild_id]['thz'].pop(user_id, None)

    async def _remove_roles(self, guild_id, *role_ids):
        table = self.tables[guild_id]['auto']
        where = reduce(
            or_,
            (table.role_id == role_id for role_id in role_ids),
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(str(
                    self.Query.from_(table).where(where).delete()
                ))

        for role_id in role_ids:
            self.cache[guild_id]['auto'].pop(role_id, None)

    async def on_guild_join(self, guild):
        auto_name = f'autoroles-{guild.id}'
        thz_name = f'thz-{guild.id}'

        self.tables[guild.id]['auto'] = Table(auto_name)
        self.tables[guild.id]['thz'] = Table(thz_name)

        self.cache[guild.id]['auto'] = {}
        self.cache[guild.id]['thz'] = {}

    async def on_guild_role_delete(self, role: Role):
        if role.id in self.cache[role.guild.id]['auto']:
            await self._remove_roles(role.guild.id, role.id)

    async def on_member_remove(self, member: Member):
        if member.id in self.cache[member.guild.id]['thz']:
            await self._remove_users(member.guild.id, member.id)


async def _setup(bot: Bot):
    await bot.wait_until_ready()
    cog = AutoRoles(bot)
    await cog._init()
    log.info("adding AutoRoles cog")
    bot.add_cog(cog)


def setup(bot: Bot):
    log.info("scheduling autoroles setup")
    bot.loop.create_task(_setup(bot))


def teardown(bot: Bot):
    log.info("removing AutoRoles cog")
    bot.remove_cog(AutoRoles.__name__)
