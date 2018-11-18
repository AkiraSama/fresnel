import asyncio
import logging
import string
from bisect import bisect_left
from functools import reduce
from operator import itemgetter, or_

from discord import Embed, Member, Message, Role
from discord.ext.commands import (
    Bot,
    BucketType,
    Context,
    command,
    cooldown,
    group,
    has_permissions,
)
from psycopg2 import IntegrityError
from pypika import Table

from fresnel.core.util import EmbedPaginator


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
            self.cache[guild.id]['auto']['reverse'] = {}
            self.cache[guild.id]['auto']['values'] = []
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
                            self.cache[guild.id]['auto'][
                                'reverse'
                            ][thz] = role.id
                            self.cache[guild.id]['auto']['values'].append(thz)
                        else:
                            cleanup.append(role_id)

                    self.cache[guild.id]['auto']['values'].sort()

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
                log.debug("allocating THz")
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
            thz = self.cache[guild_id]['auto'].pop(role_id, None)
            self.cache[guild_id]['auto']['reverse'].pop(thz)
            self.cache[guild_id]['auto']['values'].remove(thz)

    async def on_guild_join(self, guild):
        auto_name = f'autoroles-{guild.id}'
        thz_name = f'thz-{guild.id}'

        self.tables[guild.id]['auto'] = Table(auto_name)
        self.tables[guild.id]['thz'] = Table(thz_name)

        self.cache[guild.id]['auto'] = {}
        self.cache[guild.id]['auto']['reverse'] = {}
        self.cache[guild.id]['auto']['values'] = []
        self.cache[guild.id]['thz'] = {}

    async def on_guild_role_delete(self, role: Role):
        if role.id in self.cache[role.guild.id]['auto']:
            await self._remove_roles(role.guild.id, role.id)

    async def on_member_remove(self, member: Member):
        if member.id in self.cache[member.guild.id]['thz']:
            await self._remove_users(member.guild.id, member.id)

    def _get_nearest_role(self, guild_id, thz: int):
        values = self.cache[guild_id]['auto']['values']
        index = bisect_left(values, thz)
        if values[index] > thz:
            raise IndexError("no applicable roles")
        return self.cache[guild_id]['auto']['reverse'][values[index]]

    async def _isolate_highest_role(self, member):
        autoroles = []
        for role in member.roles:
            if role.id in self.cache[member.guild.id]['auto']:
                autoroles.append(role)
        autoroles.sort(
            key=lambda role: self.cache[member.guild.id]['auto'][role.id],
            reverse=True,
        )
        if autoroles:
            await member.remove_roles(*autoroles[1:])
            return autoroles[0]
        return None

    @command(aliases=('lb',))
    @cooldown(1, 10.0, BucketType.channel)
    async def leaderboard(self, ctx: Context):
        """Display THz counts for this server."""

        ranks = sorted(
            (
                (thz, user_id)
                for user_id, thz
                in self.cache[ctx.guild.id]['thz'].items()
            ),
            reverse=True,
        )

        pages = EmbedPaginator(ctx, f"THz counts for {ctx.guild.name}...")
        for index, (thz, user_id) in enumerate(ranks, start=1):
            member = ctx.guild.get_member(user_id)
            if member:
                pages.add_line(f"{index}. {member.mention} - {thz} THz")
            else:
                pages.add_line(f"{index}. User {user_id} - {thz} THz")

        await pages.send_to()

    @command(aliases=('level', 'xp'))
    @cooldown(1, 5.0, BucketType.user) 
    async def thz(self, ctx: Context, member: Member = None):
        """Display your THz for this server."""

        if not member:
            member = ctx.author

        try:
            thz = self.cache[ctx.guild.id]['thz'][member.id]
        except KeyError:
            await ctx.send("Untracked user!")
            return

        try:
            role = ctx.guild.get_role(
                self._get_nearest_role(ctx.guild.id, thz)
            )
        except IndexError:
            role = None

        ranks = sorted(
            (
                (thz, user_id)
                for user_id, thz
                in self.cache[ctx.guild.id]['thz'].items()
            ),
            reverse=True,
        )

        for index, (thz, user_id) in enumerate(ranks, start=1):
            if user_id == member.id:
                rank = index
                break

        embed=Embed(
            title=f"{member.name}'s THz for {ctx.guild.name}",
            description=(
                f"**Total**: {thz} THz\n"
                f"**Role**: {role.mention if role else 'N/A'}\n"
                f"**Rank**: #{rank}/{len(self.cache[ctx.guild.id]['thz'])}\n"
            ),
        ).set_thumbnail(
            url=member.avatar_url
        )

        await ctx.send(embed=embed)

    @group(invoke_without_command=True, aliases=('autoroles',))
    @has_permissions(manage_roles=True)
    async def autorole(self, ctx: Context):
        """Manage autoroles."""

        await ctx.send(await self.bot.get_help_message(ctx))

    @autorole.command(name='add')
    async def autorole_add(self, ctx: Context, thz: int, *, role):
        """Add a role to the autorole registration."""

        role = self.bot.convert_roles(ctx, role)[0]
        if thz in self.cache[ctx.guild.id]['auto']['reverse']:
            await ctx.send('There is already a role registered for this THz '
                           'value.')

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                table = self.tables[ctx.guild.id]['auto']
                try:
                    await cur.execute(str(
                        self.Query.into(table).insert(role.id, thz)
                    ))
                except IntegrityError:
                    await cur.execute(str(
                        self.Query.update(table).set(
                            table.thz, thz,
                        ).where(
                            table.role_id == role.id
                        )
                    ))

        self.cache[ctx.guild.id]['auto'][role.id] = thz
        self.cache[ctx.guild.id]['auto']['reverse'][thz] = role.id
        self.cache[ctx.guild.id]['auto']['values'].append(thz)
        self.cache[ctx.guild.id]['auto']['values'].sort()
        await ctx.send(f'Registered role "{role}" for {thz:,} THz.')

        async with self.pool.acquire() as conn:
            guild_id = ctx.guild.id
            table = self.tables[guild_id]['thz']
            for member in ctx.guild.members:
                highest = await self._isolate_highest_role(member)
                try:
                    member_thz = self.cache[guild_id]['thz'][member.id]
                except KeyError:
                    continue
                if self._get_nearest_role(
                        guild_id,
                        self.cache[guild_id]['thz'][member.id]
                ) >= thz:
                    await member.remove_roles(highest)
                    await member.add_roles(role)
                    continue

                if (highest == role) and (
                    self.cache[guild_id]['thz'][member.id] < thz
                ):
                    self.cache[guild_id]['thz'][member.id] = thz
                    async with conn.cursor() as cur:
                        try:
                            await cur.execute(str(
                                self.Query.into(
                                    table
                                ).insert(
                                    user_id,
                                    self.cache[guild_id]['thz'][member.id],
                                )
                            ))
                        except IntegrityError:
                            await cur.execute(str(
                                self.Query.update(table).set(
                                    table.thz,
                                    self.cache[guild_id]['thz'][member.id],
                                ).where(
                                    table.user_id == member.id
                                )
                            ))

    @autorole.command(name='remove')
    async def autorole_remove(self, ctx: Context, *, role):
        """Remove a role from the autorole registration."""

        role = self.bot.convert_roles(ctx, role)[0]

        await self._remove_roles(ctx.guild.id, role.id)

        await ctx.send(f'Unregistered role "{role}".')

    @autorole.command(name='list')
    async def autorole_list(self, ctx: Context):
        """List all available autoroles."""

        roles = self.cache[ctx.guild.id]['auto']
        if not roles:
            await ctx.send("No autoroles registered.")
            return

        roles = sorted(
            (
                (ctx.guild.get_role(role_id), thz)
                for role_id, thz
                in roles.items()
                if isinstance(role_id, int)
            ),
            key=itemgetter(1),
        )

        pages = EmbedPaginator(ctx, f"{ctx.guild.name} autoroles...")
        for index, (role, thz) in enumerate(roles, start=1):
            pages.add_line(f'{index}. {role.mention} - {thz} THz')

        await pages.send_to()


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
