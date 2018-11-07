import asyncio
import logging
import sys
import traceback
from datetime import datetime

from discord import Color, Embed
from discord.ext.commands import Bot, Context


log = logging.getLogger(__name__)


def setup(bot: Bot):
    log.info('registering on_error event')

    @bot.event
    async def on_error(event: str, *args, **kwargs):
        tcb = ''.join(traceback.format_exception(*sys.exc_info()))
        log.warning(f"Exception in event {event}:\n{tcb}")

    @bot.event
    async def on_command_error(ctx: Context, exception: Exception):
        if ctx.command is None:
            msg = await ctx.send("No such command.")
            await asyncio.sleep(5)
            await msg.delete()
            return

        tcb = ''.join(traceback.format_exception(
            type(exception),
            exception,
            exception.__traceback__,
        ))
        log.warning(f"Exception in command {ctx.command.name}:\n{tcb}")
        await ctx.send(embed=Embed(
            title=type(exception).__name__,
            description=str(exception),
            timestamp=datetime.now(),
            color=Color.red(),
        ))

    async def get_help_message(ctx: Context):
        return ''.join(
            await ctx.bot.formatter.format_help_for(ctx, ctx.command)
        )
    bot.get_help_message = get_help_message
