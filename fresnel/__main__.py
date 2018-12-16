#!/usr/bin/env python3.6

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from discord.ext import commands

from fresnel import config, constants


parser = argparse.ArgumentParser(
    prog='fresnel',
    description=constants.DESCRIPTION,
)
parser.add_argument(
    '--config',
    default=constants.DEFAULT_CONFIG_PATH,
    type=Path,
    help="path to configuration file",
    metavar='PATH',
    dest='config_filepath',
)
parser.add_argument(
    '--token',
    help="bot user token",
)
parser.add_argument(
    '--owner',
    type=int,
    help="bot owner user id",
    metavar='SNOWFLAKE_ID',
    dest='owner_id',
)
parser.add_argument(
    '-v', '--verbose',
    action='store_true',
    help="increase output verbosity",
)


def main(cfg):
    """fresnel's main method"""

    # colored logging
    if cfg.get('log_colors', False, "use ANSI colored logging"):
        for level, color in constants.LOGGING_COLORS:
            name = logging.getLevelName(level)
            logging.addLevelName(
                level,
                f'{color}{name}{constants.ANSI_RESET}',
            )
        format_str = constants.LOG_FORMAT_STR_COLOR
    else:
        format_str = constants.LOG_FORMAT_STR

    # stdout StreamHandler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(
        logging.DEBUG
        if cfg['verbose']
        else logging.INFO
    )
    formatter = logging.Formatter(
        fmt=format_str,
        datefmt='%m-%d %H:%M:%S',
        style='{',
    )
    console_handler.setFormatter(formatter)

    # set up the root logger
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)
    log.addHandler(console_handler)

    # check for a token
    token = cfg.get('token', "bot user application token")
    if not token:
        log.error("please add your bot token to your configuration "
                  "file or pass it via the command line")
        return

    # setup and run the bot
    bot = commands.Bot(
        command_prefix=commands.when_mentioned_or(
            cfg.get('default_prefix', ',',
                    "default command prefix when unconfigured")
        ),
        description=constants.DESCRIPTION,
        owner_id=cfg.get('owner_id', comment="owner discord user ID"),
    )
    bot._config = cfg

    loop = asyncio.get_event_loop()
    
    try:
        bot.load_extension('fresnel.core.error')
        bot.load_extension('fresnel.core.db')
        bot.load_extension('fresnel.core.cache')
        bot.load_extension('fresnel.core.extman')

        loop.run_until_complete(bot.start(token))
    except:  # noqa: E722
        loop.run_until_complete(bot.logout())
        raise
    finally:
        loop.close()


if __name__ == '__main__':
    # parse args and get configuration info
    args = parser.parse_args(sys.argv[1:])
    cfg = config.ConfigNamespace(args.config_filepath, args)

    main(cfg)
