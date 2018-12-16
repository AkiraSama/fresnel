import asyncio
import logging
import string
from bisect import bisect_right, insort_right
from functools import reduce
from operator import or_

from discord import Embed, Guild, Member, Message, Role
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

ROLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS "{name}" (
    role_id BIGINT NOT NULL,
    thz BIGINT NOT NULL,
    PRIMARY KEY (role_id)
)
"""

THZ_SCHEMA = """
CREATE TABLE IF NOT EXISTS "{name}" (
    user_id BIGINT NOT NULL,
    thz BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id)
)
"""

CHARS = frozenset(string.ascii_letters + string.punctuation)

backup_flag = asyncio.Event()
backup_flag.set()


class AutoRoleCache:
    def __init__(self):
        self.role_cache = {}
        self.reverse_role_cache = {}
        self.values = []

    def __bool__(self):
        return bool(self.role_cache)

    def __contains__(self, role_id: int):
        return role_id in self.role_cache

    def items(self):
        return self.role_cache.items()

    def has_thz(self, thz: int):
        return thz in self.reverse_role_cache

    def nearest_thz(self, thz: int):
        index = bisect_right(self.values, thz) - 1
        return self.values[index] if index >= 0 else None

    def get_role_id(self, thz: int):
        return self.reverse_role_cache[thz]

    def get_nearest_role_id(self, thz: int):
        thz = self.nearest_thz(thz)
        if thz is not None:
            return self.reverse_role_cache[thz]
        return None

    def find_role_ids(self, role_set: set):
        return self.role_cache.keys() & role_set

    def find_highest_role_id(self, role_set: set):
        intersect = self.find_role_ids(role_set)
        if intersect:
            for thz in reversed(self.values):
                if self.reverse_role_cache[thz] in intersect:
                    return self.reverse_role_cache[thz]
        return None

    def add_role(self, role_id: int, thz: int):
        if thz in self.reverse_role_cache:
            raise ValueError("there is already a role id for this THz value")

        try:
            self.remove_role(role_id)
        except ValueError:
            pass

        self.role_cache[role_id] = thz
        self.reverse_role_cache[thz] = role_id
        insort_right(self.values, thz)

    def remove_role(self, role_id: int):
        if role_id in self.role_cache:
            old_thz = self.role_cache[role_id]
            del self.role_cache[role_id]
            del self.reverse_role_cache[old_thz]
            self.values.remove(old_thz)
        else:
            raise ValueError("no such role id")


class AutoRoles:
    THZ_INTERVAL = 120

    def __init__(self, bot: Bot):
        self.bot = bot
        self.pool = bot._db_pool
        self.Query = bot._db_Query
        self.tables = {}
        self.role_cache = {}
        self.thz_cache = {}
        self.user_cache = {}
        self.time_cache = {}
        self.ptask = None

    async def _init(self):
        for guild in self.bot.guilds:
            role_name = f'autoroles-{guild.id}'
            thz_name = f'thz-{guild.id}'

            self.tables[guild.id] = {}
            self.tables[guild.id]['role'] = role_table = Table(role_name)
            self.tables[guild.id]['thz'] = thz_table = Table(thz_name)

            self.role_cache[guild.id] = AutoRoleCache()
            self.thz_cache[guild.id] = {}
            self.user_cache[guild.id] = {}

            self.time_cache[guild.id] = {}

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        ROLE_SCHEMA.format(name=role_name)
                    )

                    await cur.execute(str(
                        self.Query.from_(role_table).select(
                            role_table.role_id, role_table.thz,
                        )
                    ))

                    cleanup = []
                    async for role_id, thz in cur:
                        role = guild.get_role(role_id)

                        if role:
                            self.role_cache[guild.id].add_role(role.id, thz)
                        else:
                            cleanup.append(role_id)

                    if cleanup:
                        await self._remove_roles(guild.id, *cleanup)

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
                    member_ids = set((
                        member.id
                        for member
                        in guild.members
                        if not member.bot
                    ))
                    async for user_id, thz in cur:
                        member_ids.discard(user_id)

                        user = guild.get_member(user_id)

                        if user:
                            self.thz_cache[guild.id][user.id] = thz
                            await self._update_user_role(cur, guild, user)
                        else:
                            cleanup.append(user_id)

                    if cleanup:
                        await self._remove_users(guild.id, *cleanup)

                    for member_id in member_ids:
                        await self.on_member_join(guild.get_member(member_id))

        await self.bot.fresnel_cache_flag.wait()

        self.ptask = self.bot.loop.create_task(
            self.periodic()
        )

    def __unload(self):
        if self.ptask:
            self.ptask.cancel()

    async def periodic(self):
        while True:
            if not backup_flag.is_set():
                break
            await asyncio.sleep(self.THZ_INTERVAL)
            try:
                log.debug("allocating THz")
                await self._periodic()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"periodic error: {e}")

    async def _periodic(self):
        time_cache = self.time_cache
        self.time_cache = {guild.id: {} for guild in self.bot.guilds}

        for guild_id, users in time_cache.items():
            for user_id, delta in users.items():
                if delta is None:
                    inc = 1
                    continue

                inc = 1 + delta.get('len', 0) + delta.get('var', 0)
                self.thz_cache[guild_id][user_id] = (
                    inc + self.thz_cache[guild_id].get(user_id, 0)
                )

        async with self.pool.acquire() as conn:
            for guild_id, users in time_cache.items():
                guild = self.bot.get_guild(guild_id)
                async with conn.cursor() as cur:
                    for user_id, delta in users.items():
                        await self._update_user_thz(
                            cur, guild_id, user_id
                        )
                        await self._update_user_role(
                            cur, guild, guild.get_member(user_id)
                        )

    async def _update_user_thz(self, cursor, guild_id, user_id):
        table = self.tables[guild_id]['thz']
        try:
            await cursor.execute(str(
                self.Query.into(
                    table
                ).insert(
                    user_id,
                    self.thz_cache[guild_id][user_id],
                )
            ))
        except IntegrityError:
            await cursor.execute(str(
                self.Query.update(table).set(
                    table.thz,
                    self.thz_cache[guild_id][user_id],
                ).where(
                    table.user_id == user_id
                )
            ))

    async def _update_user_role(self, cursor, guild, member):
        role_id = self.role_cache[guild.id].get_nearest_role_id(
            self.thz_cache[guild.id].get(member.id, 0)
        )

        if self.user_cache[guild.id].get(member.id) == role_id:
            return

        role_ids = self.role_cache[guild.id].find_role_ids(
            frozenset((role.id for role in member.roles))
        )
        role_ids.discard(role_id)

        if role_id:
            await member.add_roles(
                guild.get_role(role_id),
                reason="Fresnel autoroles",
            )
        await member.remove_roles(
            *(guild.get_role(rid) for rid in role_ids),
            reason="Fresnel autoroles",
        )

        self.user_cache[guild.id][member.id] = role_id

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
            self.time_cache[guild_id].pop(user_id, None)
            self.thz_cache[guild_id].pop(user_id, None)
            self.user_cache[guild_id].pop(user_id, None)

    async def _remove_roles(self, guild_id, *role_ids):
        table = self.tables[guild_id]['role']
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
            try:
                self.role_cache[guild_id].remove_role(role_id)
            except ValueError:
                pass

    def _get_user_ranks(self, guild_id: int):
        return sorted(
            (
                (thz, user_id)
                for user_id, thz
                in self.thz_cache[guild_id].items()
            ),
            reverse=True,
        )

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
        if len(CHARS & var) >= 13:
            delta['var'] = min(delta.get('var', 0) + 1, 3)

        latest = delta.get('latest', (length, var, 0))
        if latest[0] == length and latest[1] == var:
            delta['latest'] = (length, var, latest[2] + 1)

        if latest[2] >= 5:
            self.time_cache[message.guild.id][message.author.id] = None
        else:
            self.time_cache[message.guild.id][message.author.id] = delta

    async def on_guild_join(self, guild: Guild):
        role_name = f'autoroles-{guild.id}'
        thz_name = f'thz-{guild.id}'

        self.tables[guild.id] = {}
        self.tables[guild.id]['auto'] = Table(role_name)
        self.tables[guild.id]['thz'] = Table(thz_name)

        self.role_cache[guild.id] = AutoRoleCache()
        self.thz_cache[guild.id] = {}
        self.user_cache[guild.id] = {}

        self.time_cache[guild.id] = {}

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    ROLE_SCHEMA.format(name=role_name)
                )

                await cur.execute(
                    THZ_SCHEMA.format(name=thz_name)
                )

    async def on_guild_remove(self, guild: Guild):
        role_name = f'autoroles-{guild.id}'
        thz_name = f'thz-{guild.id}'

        del self.tables[guild.id]

        del self.role_cache[guild.id]
        del self.thz_cache[guild.id]
        del self.user_cache[guild.id]

        del self.time_cache[guild.id]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f'DROP TABLE "{role_name}"'
                )

                await cur.execute(
                    f'DROP TABLE "{thz_name}"'
                )

    async def on_guild_role_delete(self, role: Role):
        if role.id in self.role_cache[role.guild.id]:
            await self._remove_roles(role.guild.id, role.id)

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    for member in role.guild.members:
                        if member.bot:
                            continue

                        await self._update_user_role(
                            cur,
                            role.guild,
                            member,
                        )

    async def on_member_remove(self, member: Member):
        await self._remove_users(member.guild.id, member.id)

    async def on_member_join(self, member: Member):
        if member.bot:
            return

        self.thz_cache[member.guild.id][member.id] = 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await self._update_user_thz(cur, member.guild.id, member.id)
                await self._update_user_role(cur, member.guild, member)

    @command(aliases=('lb',))
    @cooldown(1, 10.0, BucketType.channel)
    async def leaderboard(self, ctx: Context):
        """Display THz counts for this server."""

        ranks = self._get_user_ranks(ctx.guild.id)

        pages = EmbedPaginator(ctx, f"THz counts for {ctx.guild.name}...")
        for index, (thz, user_id) in enumerate(ranks, start=1):
            member = ctx.guild.get_member(user_id)
            if member:
                pages.add_line(f"{index}. {member.mention} - {thz:,} THz")
            else:
                pages.add_line(f"{index}. user {user_id} - {thz:,} THz")

        await pages.send_to()

    @command(aliases=('level', 'xp'))
    @cooldown(1, 5.0, BucketType.user)
    async def thz(self, ctx: Context, member: Member = None):
        """Display your THz for this server."""

        if not member:
            member = ctx.author

        try:
            thz = self.thz_cache[ctx.guild.id][member.id]
        except KeyError:
            await ctx.send("Untracked user!")
            return

        role_id = self.user_cache[ctx.guild.id].get(member.id)
        role = ctx.guild.get_role(role_id) if role_id else None

        ranks = self._get_user_ranks(ctx.guild.id)

        for index, (thz, user_id) in enumerate(ranks, start=1):
            if user_id == member.id:
                rank = index
                break

        embed = Embed(
            title=f"{member.name}'s THz for {ctx.guild.name}",
            description=(
                f"**Total**: {thz:,} THz\n"
                f"**Role**: {role.mention if role else 'N/A'}\n"
                f"**Rank**: #{rank}/{len(self.thz_cache[ctx.guild.id])}\n"
            ),
        ).set_thumbnail(
            url=member.avatar_url
        )

        await ctx.send(embed=embed)

    @group(invoke_without_command=True, aliases=('autoroles',))
    async def autorole(self, ctx: Context):
        """Manage autoroles."""

        #await ctx.send(await self.bot.get_help_message(ctx))

    @autorole.command(name='list')
    async def autorole_list(self, ctx: Context):
        """List all available autoroles."""

        roles = self.role_cache[ctx.guild.id]
        if not roles:
            await ctx.send("No autoroles registered.")
            return

        roles = (
            (
                ctx.guild.get_role(
                    self.role_cache[ctx.guild.id].get_role_id(thz)
                ),
                thz,
            )
            for thz
            in self.role_cache[ctx.guild.id].values
        )

        pages = EmbedPaginator(ctx, f"{ctx.guild.name} autoroles...")
        for index, (role, thz) in enumerate(roles, start=1):
            pages.add_line(f'{index}. {role.mention} - {thz:,} THz')

        await pages.send_to()

    @autorole.command(name='add')
    @has_permissions(manage_roles=True)
    async def autorole_add(self, ctx: Context, thz: int, *, role):
        """Add a role to the autorole registration."""

        if thz < 0:
            await ctx.send("You can't have negative THz!")
            return

        role = self.bot.convert_roles(ctx, role)[0]
        if self.role_cache[ctx.guild.id].has_thz(thz):
            await ctx.send("There is already a role registered for this Thz "
                           "value.")
            return

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                table = self.tables[ctx.guild.id]['role']
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

        self.role_cache[ctx.guild.id].add_role(role.id, thz)
        await ctx.send(f'Registered role "{role}" for {thz:,} Thz.')

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for member in ctx.guild.members:
                    if member.bot:
                        continue

                    if (
                            self.role_cache[ctx.guild.id].find_highest_role_id(
                                frozenset((r.id for r in member.roles))
                            )
                            == role.id
                    ):
                        if (
                                self.thz_cache[ctx.guild.id].get(member.id, 0)
                                < thz
                        ):
                            self.thz_cache[ctx.guild.id][member.id] = thz
                            await self._update_user_thz(
                                cur,
                                ctx.guild.id,
                                member.id,
                            )

                    await self._update_user_role(
                        cur,
                        ctx.guild,
                        member,
                    )

    @autorole.command(name='remove')
    @has_permissions(manage_roles=True)
    async def autorole_remove(self, ctx: Context, *, role):
        """Remove a role from the autorole registration."""

        role = self.bot.convert_roles(ctx, role)[0]

        await self._remove_roles(ctx.guild.id, role.id)

        await ctx.send(f'Unregistered role "{role}".')

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for member in ctx.guild.members:
                    if member.bot:
                        continue

                    await self._update_user_role(
                        cur,
                        ctx.guild,
                        member,
                    )

    @command(aliases=('setxp',))
    @has_permissions(manage_roles=True)
    async def setthz(self, ctx: Context, member: Member, thz: int):
        """Set the THz of a user for your guild."""

        if thz < 0:
            await ctx.send("You can't have a negative THz!")
            return

        self.thz_cache[ctx.guild.id][member.id] = thz
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await self._update_user_thz(cur, ctx.guild.id, member.id)
                await self._update_user_role(cur, ctx.guild, member)

        await ctx.send(f"{member.name}'s THz set to {thz:,} THz.")


async def _setup(bot: Bot):
    await bot.wait_until_ready()
    try:
        cog = AutoRoles(bot)
        await cog._init()
        log.info("adding AutoRoles cog")
        bot.add_cog(cog)
    except:  # noqa: E722
        backup_flag.clear()
        raise


def setup(bot: Bot):
    log.info("scheduling autoroles setup")
    bot.loop.create_task(_setup(bot))


def teardown(bot: Bot):
    backup_flag.clear()
    log.info("removing AutoRoles cog")
    bot.remove_cog(AutoRoles.__name__)
