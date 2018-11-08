import csv
import logging
from functools import reduce
from io import StringIO
from operator import or_

from discord import Message
from discord.ext.commands import (
    Bot,
    Context,
    group,
    has_permissions,
    when_mentioned_or,
)
from psycopg2 import IntegrityError
from pypika import Table


log = logging.getLogger(__name__)


SCHEMA = '''
CREATE TABLE IF NOT EXISTS "prefix" (
    guild_id BIGINT NOT NULL,
    prefixes VARCHAR(255),
    PRIMARY KEY (guild_id)
)
'''


class PrefixManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.pool = bot._db_pool
        self.Query = bot._db_Query
        self.table = Table('prefix')
        self.cache = {}
        self.default_prefix = bot.command_prefix

    def __unload(self):
        self.bot.command_prefix = self.default_prefix

    async def _init(self):
        guild_ids = set(guild.id for guild in self.bot.guilds)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SCHEMA)

                await cur.execute(str(
                    self.Query.from_(self.table).select(
                        self.table.guild_id,
                        self.table.prefixes,
                    )
                ))

                cleanup = []
                async for guild_id, prefixes in cur:
                    if guild_id in guild_ids:
                        try:
                            self.cache[guild_id] = next(
                                csv.reader(StringIO(prefixes))
                            )
                        except StopIteration:
                            pass
                    else:
                        cleanup.append(guild_id)

                if cleanup:
                    where = reduce(
                        or_,
                        (self.table.guild_id == guild_id
                         for guild_id
                         in cleanup),
                    )

                    await cur.execute(str(
                        self.Query.form_(table).where(where).delete()
                    ))

        def get_prefix(bot: Bot, message: Message):
            prefixes = self.cache.get(message.guild.id)
            if prefixes:
                return when_mentioned_or(*prefixes)(bot, message)
            if callable(self.default_prefix):
                return self.default_prefix(bot, message)
            return self.default_prefix

        self.bot.command_prefix = get_prefix

    @group(invoke_without_command=True)
    async def prefix(self, ctx: Context):
        """Manage custom command prefixes."""

        await ctx.send(await self.bot.get_help_message(ctx))

    @prefix.command(name='list')
    async def prefix_list(self, ctx: Context):
        """List custom command prefixes."""

        prefixes = self.cache.get(ctx.guild.id)
        if not prefixes:
            await ctx.send("No prefixes configured!")
            return

        await ctx.send(f"Guild prefixes: `{'`, `'.join(prefixes)}`")

    @prefix.command(name='add')
    @has_permissions(manage_messages=True)
    async def prefix_add(self, ctx: Context, prefix: str):
        """Add a new custom command prefix."""

        new_prefixes = self.cache.get(ctx.guild.id, [])

        if prefix in new_prefixes:
            await ctx.send("This prefix has already been added.")
            return

        new_prefixes.append(prefix)
        
        row = StringIO()
        csv.writer(row).writerow(new_prefixes)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(str(
                        self.Query.into(self.table).insert(
                            ctx.guild.id, row.getvalue(),
                        )
                    ))
                except IntegrityError:
                    await cur.execute(str(
                        self.Query.update(self.table).set(
                            self.table.prefixes, row.getvalue(),
                        ).where(
                            self.table.guild_id == ctx.guild.id
                        )
                    ))

        self.cache[ctx.guild.id] = new_prefixes
        await ctx.send("Added new prefix.")

    @prefix.command(name='remove')
    @has_permissions(manage_messages=True)
    async def prefix_remove(self, ctx: Context, prefix: str):
        """Remove and existing custom command prefix."""

        new_prefixes = self.cache.get(ctx.guild.id, [])

        if not new_prefixes:
            await ctx.send("No configured prefixes.")
            return

        try:
            new_prefixes.remove(prefix)
        except ValueError:
            await ctx.send("No such prefix exists.")
            return
        
        row = StringIO()
        csv.writer(row).writerow(new_prefixes)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(str(
                    self.Query.update(self.table).set(
                        self.table.prefixes, row.getvalue(),
                    ).where(
                        self.table.guild_id == ctx.guild.id
                    )
                ))

        self.cache[ctx.guild.id] = new_prefixes
        await ctx.send("Prefix removed.")


async def _setup(bot: Bot):
    await bot.wait_until_ready()
    cog = PrefixManager(bot)
    await cog._init()
    log.info("adding PrefixManager cog")
    bot.add_cog(cog)


def setup(bot: Bot):
    log.info("scheduling prefix setup")
    bot.loop.create_task(_setup(bot))


def teardown(bot: Bot):
    log.info("removing PrefixManager cog")
    bot.remove_cog(PrefixManager.__name__)
