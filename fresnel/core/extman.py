import logging
from pathlib import Path

from discord.ext.commands import Bot, Context, command, is_owner


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

    async def __global_check(self, ctx: Context):
        return is_owner

    def _get_name(self, ext_name):
        return f'{self.ext_prefix}.{ext_name}'

    @command()
    async def enable(self, ctx: Context, ext_name: str):
        """load extension by name"""

        if self._get_name(ext_name) in self.bot.extensions:
            await ctx.send(f"extension `{ext_name}` is already loaded")
        else:
            log.info(f"attempting to load extension {ext_name}")
            self.bot.load_extension(self._get_name(ext_name))
            await ctx.send(f"loaded extension `{ext_name}`")
            log.info(f"loaded extension {ext_name}")

    @command()
    async def disable(self, ctx: Context, ext_name: str):
        """unload extension by name"""

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

    @command()
    async def reload(self, ctx: Context, ext_name: str):
        """reload extension by name"""

        if await self.disable.callback(self, ctx, ext_name):
            await self.enable.callback(self, ctx, ext_name)


def setup(bot: Bot):
    log.info("loading ExtensionManager cog")
    bot.add_cog(ExtensionManager(bot))
