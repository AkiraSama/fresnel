import logging

from discord.ext.commands import (
    Bot,
    Cog,
    Context,
    group,
    has_permissions,
    when_mentioned_or,
)


log = logging.getLogger(__name__)

KEY_NAME = 'reactroles'  # {guild_id,}
GUILD_KEY = 'reactroles:{guild_id}'  # {message_id,}
EMOJI_KEY = 'reactroles:{guild_id}:{message_id}'
# {emoji_id: role_id}


class ReactRoles(Cog):
    def __init__(self, bot: Bot):
        self.bot = bot
        self.redis = bot.redis_pool

    async def _init(self):
        print(await self.redis.sinter(KEY_NAME))
        diff = await self.redis.sinter(KEY_NAME) - set(
            str(guild.id)
            for guild
            in self.bot.guilds
        )

        for guild_id in diff:
            await self.remove_guild(guild_id)

    async def remove_guild(self, guild_id):
        with await self.redis as conn:
            remove_tr = conn.multi_exec()

            remove_tr.srem(KEY_NAME, guild_id)
            remove_tr.delete(GUILD_KEY.format(guild_id=guild_id))
            remove_tr.delete(GUILD_KEY_REVERSE.format(
                guild_id=guild_id,
            ))

            await remove_tr.execute()

    @group(aliases=('rr',))
    @has_permissions(manage_roles=True)
    async def reactroles(self, ctx: Context):
        """Manage roles assigned by reaction."""

        await ctx.send(await self.bot.get_help_message(ctx))

    @reactroles.command(name='list')
    async def reactroles_list(self, ctx: Context):
        ...


async def _setup(bot: Bot):
    await bot.wait_until_ready()
    cog = ReactRoles(bot)
    await cog._init()
    log.info("adding ReactRoles")
    bot.add_cog(cog)


def setup(bot: Bot):
    log.info("scheduling prefix setup")
    bot.loop.create_task(_setup(bot))


def teardown(bot: Bot):
    log.info("removing ReactRoles cog")
    bot.remove_cog(ReactRoles.__name__)
