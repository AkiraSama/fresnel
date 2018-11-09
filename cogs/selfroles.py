import logging
import re
import shlex
from functools import reduce
from operator import attrgetter, or_

from discord import Color, Embed, Role
from discord.ext.commands import (
    BadArgument,
    Bot,
    Context,
    NoPrivateMessage,
    command,
    group,
    has_permissions,
)
from psycopg2 import IntegrityError
from pypika import Table

from fresnel.core.util import EmbedPaginator


log = logging.getLogger(__name__)

SCHEMA = '''
CREATE TABLE IF NOT EXISTS "{name}" (
    role_id BIGINT NOT NULL,
    PRIMARY KEY (role_id)
)
'''

ID_MATCH = re.compile(r'([0-9]{15,21})$')
ROLE_ID_MATCH = re.compile(r'<@&([0-9]+)>$')


class SelfRoles:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.pool = bot._db_pool
        self.Query = bot._db_Query
        self.tables = {}
        self.cache = {}
        self.name_cache = {}

    async def _init(self):
        for guild in self.bot.guilds:
            name = f'selfroles-{guild.id}'
            self.tables[guild.id] = table = Table(name)

            self.cache[guild.id] = set()
            self.name_cache[guild.id] = {
                role.name.upper(): role.id for role in guild.roles
            }

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        SCHEMA.format(name=name)
                    )

                    await cur.execute(str(
                        self.Query.from_(table).select(table.role_id)
                    ))

                    cleanup = []
                    async for role_id, in cur:
                        role = guild.get_role(role_id)

                        if role:
                            self.cache[guild.id].add(role.id)
                        else:
                            cleanup.append(role_id)

                    if cleanup:
                        await self._remove_roles(guild.id, *cleanup)

    async def on_guild_join(self, guild):
        name = f'selfroles-{guild.id}'
        self.tables[guild.id] = Table(name)

        self.cache[guild.id] = set()
        self.name_cache[guild.id] = {
            role.name.upper(): role.id for role in guild.roles
        }

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    SCHEMA.format(name=name)
                )

    def _convert_roles(self, ctx: Context, full_message: str):
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
                        self.name_cache[guild.id].get(arg.upper())
                    )

                if result is None:
                    raise BadArgument(f'Role "{arg}" not found.')
                results.append(result)
        except BadArgument:
            if not results:
                result = guild.get_role(
                    self.name_cache[guild.id].get(full_message.upper())
                )

                if result is None:
                    raise
                results.append(result)
            else:
                raise

        return results

    @group(invoke_without_command=True)
    @has_permissions(manage_roles=True)
    async def roleman(self, ctx: Context):
        """Manage selfroles."""

        await ctx.send(await self.bot.get_help_message(ctx))

    @roleman.command(name='add')
    async def roleman_add(self, ctx: Context, *, roles):
        """Add roles to the selfrole registration."""

        roles = self._convert_roles(ctx, roles)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for role in roles:
                    try:
                        await cur.execute(str(
                            self.Query.into(
                                self.tables[ctx.guild.id]
                            ).insert(role.id)
                        ))
                    except IntegrityError:
                        pass
                    else:
                        self.cache[ctx.guild.id].add(role.id)

        pages = EmbedPaginator(ctx, "Registered the following roles...",
                               color=Color.green())
        for role in roles:
            pages.add_line(role.mention)

        await pages.send_to()

    async def _remove_roles(self, guild_id, *role_ids):
        table = self.tables[guild_id]
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
            self.cache[guild_id].discard(role_id)

    @roleman.command(name='remove')
    async def roleman_remove(self, ctx: Context, *, roles):
        """Remove roles from the selfrole registration."""

        roles = self._convert_roles(ctx, roles)

        await self._remove_roles(ctx.guild.id, *(role.id for role in roles))

        pages = EmbedPaginator(ctx, "Unregistered the following roles...",
                               color=Color.green())
        for role in roles:
            pages.add_line(role.mention)

        await pages.send_to()

    async def on_guild_role_delete(self, role: Role):
        if role.id in self.cache[role.guild.id]:
            await self._remove_roles(role.guild.id, role.id)

    async def on_guild_role_update(self, before: Role, after: Role):
        if before.name != after.name:
            self.name_cache[before.guild.id][after.name.upper()] = before.id
            del self.name_cache[before.guild.id][before.name.upper()]

    @command()
    async def listroles(self, ctx: Context):
        """List all available selfroles."""

        roles = self.cache.get(ctx.guild.id)
        if not roles:
            await ctx.send("No roles registered.")
            return

        roles = sorted(
            (
                ctx.guild.get_role(role_id)
                for role_id
                in roles
            ),
            key=attrgetter('position'),
            reverse=True,
        )

        pages = EmbedPaginator(ctx, "Available selfroles...")
        for line, role in enumerate(roles, start=1):
            pages.add_line(role.mention)
            if line % 10 == 0:
                pages.close_page()

        await pages.send_to()

    @command()
    async def addrole(self, ctx: Context, *, roles):
        """Add a role to yourself."""

        roles = self._convert_roles(ctx, roles)

        available = []
        unavailable = []
        for role in roles:
            if role.id in self.cache[ctx.guild.id]:
                available.append(role)
            else:
                unavailable.append(role)

        if unavailable:
            await ctx.send(embed=Embed(
                title="Unavailable for selfroling...",
                description=', '.join(role.mention for role in unavailable),
                color=Color.red(),
            ))

        if available:
            await ctx.author.add_roles(*available,
                                       reason="Fresnel selfroling")
            await ctx.send(embed=Embed(
                title="Roles applied...",
                description=', '.join(role.mention for role in available),
                color=Color.green(),
            ))

    @command()
    async def removerole(self, ctx: Context, *, roles):
        """Remove a role from yourself."""

        roles = self._convert_roles(ctx, roles)

        available = []
        unavailable = []
        for role in roles:
            if role.id in self.cache[ctx.guild.id]:
                available.append(role)
            else:
                unavailable.append(role)

        if unavailable:
            await ctx.send(embed=Embed(
                title="Unavailable for selfroling...",
                description=', '.join(role.mention for role in unavailable),
                color=Color.red(),
            ))

        if available:
            await ctx.author.remove_roles(*available,
                                          reason="Fresnel selfroling")
            await ctx.send(embed=Embed(
                title="Roles removed...",
                description=', '.join(role.mention for role in available),
                color=Color.green(),
            ))


async def _setup(bot: Bot):
    await bot.wait_until_ready()
    cog = SelfRoles(bot)
    await cog._init()
    log.info("adding SelfRoles cog")
    bot.add_cog(cog)


def setup(bot: Bot):
    log.info("scheduling selfroles setup")
    bot.loop.create_task(_setup(bot))


def teardown(bot: Bot):
    log.info("removing SelfRoles cog")
    bot.remove_cog(SelfRoles.__name__)
