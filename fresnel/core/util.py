import asyncio
import logging
from collections import OrderedDict
from enum import IntEnum
from functools import partial

from discord import Embed
from discord.abc import Messageable
from discord.ext.commands import Context, Paginator


log = logging.getLogger(__name__)


class EmbedPaginator:
    class Navigation(IntEnum):
        FIRST = 0
        BACK = 1
        NEXT = 2
        LAST = 3

    EMOJIS = OrderedDict((
        ('⏮', Navigation.FIRST),
        ('⏪', Navigation.BACK),
        ('⏩', Navigation.NEXT),
        ('⏭', Navigation.LAST),
    ))

    def __init__(self, ctx: Context, base_title: str, color=None):
        self.ctx = ctx
        self.title = base_title
        self.attrs = {}
        if color:
            self.attrs['color'] = color
        self.paginator = Paginator(prefix='', suffix='', max_size=2048)

    def add_line(self, line='', *, empty=False):
        self.paginator.add_line(line, empty=empty)

    def close_page(self):
        self.paginator.close_page()

    def _check(self, page, pages, reaction, user):
        reaction = reaction.emoji
        if user.id != self.ctx.author.id:
            return False
        if reaction not in self.EMOJIS:
            return False
        if page == 1 and self.EMOJIS[reaction] <= self.Navigation.BACK:
            return False
        if page == pages and self.EMOJIS[reaction] >= self.Navigation.NEXT:
            return False
        return True

    async def send_to(self, dest: Messageable = None):
        if not dest:
            dest = self.ctx

        pages = self.paginator.pages
        page = 1

        msg = await dest.send(embed=Embed(
            title=f'{self.title} ({page}/{len(pages)})',
            description=pages[page - 1],
            **self.attrs,
        ))

        if len(pages) == 1:
            return

        for emoji in self.EMOJIS:
            await msg.add_reaction(emoji)

        try:
            while True:
                check = partial(self._check, page, len(pages))
                reaction, user = await self.ctx.bot.wait_for(
                    'reaction_add',
                    check=check,
                    timeout=30.0,
                )

                action = self.EMOJIS[reaction.emoji]
                if action is self.Navigation.FIRST:
                    page = 1
                elif action is self.Navigation.BACK:
                    page -= 1
                elif action is self.Navigation.NEXT:
                    page += 1
                elif action is self.Navigation.LAST:
                    page = len(pages)

                await msg.edit(embed=Embed(
                    title=f'{self.title} ({page}/{len(pages)})',
                    description=pages[page - 1],
                    **self.attrs,
                ))
        except asyncio.TimeoutError:
            for emoji in self.EMOJIS:
                await msg.remove_reaction(emoji, msg.author)
