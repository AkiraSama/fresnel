import logging
from pathlib import Path

from discord.ext.commands import Bot, Context, group, is_owner


log = logging.getLogger(__name__)


class ExtensionManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.ext_dir = Path(bot._config.get(
            'ext_dir', './cogs/',
            "relative path to extensions import directory",
        ))
        self.ext_prefix = '.'.join(self.ext_dir.parts)

        for ext in self.ext_dir.iterdir():
            if ext.suffix == '.py':
                path = '.'.join(ext.with_suffix('').parts)
                log.info(f"attempting to load extension '{path}'")
                bot.load_extension(path)

    def _get_name(self, ext_name):
        return f'{self.ext_prefix}.{ext_name}'

    @group(invoke_without_command=True)
    @is_owner()
    async def ext(self, ctx: Context, ext_name: str):
        """Manage extensions."""

        await ctx.send(await self.bot.get_help_message(ctx))

    @ext.command(name='enable')
    async def ext_enable(self, ctx: Context, ext_name: str):
        """Load or enable an extension by name."""

        if self._get_name(ext_name) in self.bot.extensions:
            await ctx.send(f"extension `{ext_name}` is already loaded")
        else:
            log.info(f"attempting to load extension {ext_name}")
            self.bot.load_extension(self._get_name(ext_name))
            await ctx.send(f"loaded extension `{ext_name}`")
            log.info(f"loaded extension {ext_name}")

    @ext.command(name='disable')
    async def ext_disable(self, ctx: Context, ext_name: str):
        """Disable an extension by name."""

        # TODO: add full disabling powers

        if self._get_name(ext_name) in self.bot.extensions:
            log.info(f"attempting to load extension {ext_name}")
            self.bot.unload_extension(self._get_name(ext_name))
            await ctx.send(f"unloaded extension `{ext_name}`")
            log.info(f"unloaded extension {ext_name}")
            return True
        else:
            await ctx.send(f"no extension named `{ext_name}` found")
            return False

    @ext.command(name='reload')
    async def ext_reload(self, ctx: Context, ext_name: str):
        """Reload an extension by name."""

        if await self.ext_disable.callback(self, ctx, ext_name):
            await self.ext_enable.callback(self, ctx, ext_name)


def setup(bot: Bot):
    log.info("loading ExtensionManager cog")
    bot.add_cog(ExtensionManager(bot))
