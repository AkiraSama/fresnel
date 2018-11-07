import logging
from functools import reduce
from operator import attrgetter, or_

from discord import Color, Embed, Role
from discord.ext.commands import (
    Bot,
    Context,
    command,
    group,
    has_permissions,
)
from psycopg2 import IntegrityError
from pypika import Table

from fresnel.core.util import EmbedPaginator


log = logging.getLogger(__name__)


SELFROLE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS "{name}" (
    role_id BIGINT NOT NULL,
    PRIMARY KEY (role_id)
)
'''


class SelfRoles:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.pool = bot._db_pool
        self.Query = bot._db_Query
        self.tables = {}
        self.cache = {}

    async def _init(self):
        for guild in self.bot.guilds:
            name = f'selfroles-{guild.id}'
            self.tables[guild.id] = table = Table(name)

            self.cache[guild.id] = set()

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        SELFROLE_SCHEMA.format(name=name)
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
                        where = reduce(
                            or_,
                            (table.role_id == role_id for role_id in cleanup),
                        )

                        await cur.execute(str(
                            self.Query.from_(table).where(where).delete()
                        ))

    @group(invoke_without_command=True)
    @has_permissions(manage_roles=True)
    async def roleman(self, ctx: Context):
        """manage selfroles"""

        await ctx.send(await self.bot.get_help_message(ctx))

    @roleman.command(name='add')
    async def roleman_add(self, ctx: Context, *roles: Role):
        if not roles:
            await ctx.send(await self.bot.get_help_message(ctx))
            return

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

    @roleman.command(name='remove')
    async def roleman_remove(self, ctx: Context, *roles: Role):
        if not roles:
            await ctx.send(await self.bot.get_help_message(ctx))
            return

        table = self.tables[ctx.guild.id]
        where = reduce(or_, (table.role_id == role.id for role in roles))

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(str(
                    self.Query.from_(table).where(where).delete()
                ))

        for role in roles:
            self.cache[ctx.guild.id].discard(role.id)

        pages = EmbedPaginator(ctx, "Unregistered the following roles...",
                               color=Color.green())
        for role in roles:
            pages.add_line(role.mention)

        await pages.send_to()

    @command()
    async def listroles(self, ctx: Context):
        roles = sorted(
            (
                ctx.guild.get_role(role_id)
                for role_id
                in self.cache[ctx.guild.id]
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
    async def addrole(self, ctx: Context, *roles: Role):
        if not roles:
            await ctx.send(await self.bot.get_help_message(ctx))
            return

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
    async def removerole(self, ctx: Context, *roles: Role):
        if not roles:
            await ctx.send(await self.bot.get_help_message(ctx))
            return

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
