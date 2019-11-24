import csv
import logging
from io import StringIO

from discord import Message
from discord.ext.commands import (
    Bot,
    Cog,
    Context,
    group,
    has_permissions,
    when_mentioned_or,
)


log = logging.getLogger(__name__)


KEY_NAME = 'prefixes'


class PrefixManager(Cog):
    def __init__(self, bot: Bot):
        self.bot = bot
        self.redis = bot.redis_pool
        self.cache = {}
        self.default_prefix = bot.command_prefix

    def __unload(self):
        self.bot.command_prefix = self.default_prefix

    async def _init(self):
        guild_ids = set(
            str(guild.id)
            for guild
            in self.bot.guilds
        )

        with await self.redis as conn:
            cache = await conn.hgetall(KEY_NAME)

            cleanup = cache.keys() - guild_ids
            cleanup_tr = conn.multi_exec()

            for guild_id, prefixes in cache.items():
                if guild_id in cleanup:
                    cleanup_tr.hdel(KEY_NAME, guild_id)
                else:
                    try:
                        self.cache[int(guild_id)] = next(
                            csv.reader(StringIO(prefixes))
                        )
                    except StopIteration:
                        cleanup_tr.hdel(KEY_NAME, guild_id)

            await cleanup_tr.execute()

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
    @has_permissions(manage_channels=True)
    async def prefix_add(self, ctx: Context, prefix: str):
        """Add a new custom command prefix."""

        new_prefixes = self.cache.get(ctx.guild.id, [])

        if prefix in new_prefixes:
            await ctx.send("This prefix has already been added.")
            return

        new_prefixes.append(prefix)

        row = StringIO()
        csv.writer(row).writerow(new_prefixes)

        await self.redis.hset(
            KEY_NAME,
            ctx.guild.id,
            row.getvalue(),
        )

        self.cache[ctx.guild.id] = new_prefixes
        await ctx.send("Added new prefix.")

    @prefix.command(name='remove')
    @has_permissions(manage_channels=True)
    async def prefix_remove(self, ctx: Context, prefix: str):
        """Remove an existing custom command prefix."""

        new_prefixes = self.cache.get(ctx.guild.id, [])

        if not new_prefixes:
            await ctx.send("No configured prefixes.")
            return

        try:
            new_prefixes.remove(prefix)
        except ValueError:
            await ctx.send("No such prefix exists.")
            return

        if new_prefixes:
            row = StringIO()
            csv.writer(row).writerow(new_prefixes)

            await self.redis.hset(
                KEY_NAME,
                ctx.guild.id,
                row.getvalue(),
            )

            self.cache[ctx.guild.id] = new_prefixes
        else:
            await self.redis.hdel(
                KEY_NAME,
                ctx.guild.id,
            )

            del self.cache[ctx.guild.id]

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
