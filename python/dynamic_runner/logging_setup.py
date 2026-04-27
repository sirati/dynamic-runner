"""Logging configuration for the runner.

Extracted verbatim from the previous `cli._setup_logging` so the prefix
behaviour for primary (`P|`), secondary (`S|`), and `--raw-logs` modes
is preserved.
"""

import argparse
import logging


def setup_logging(args_list: list[str]) -> logging.Logger:
    """Configure the root logger from the early-arg-parsed flags.

    Looks at `--debug`, `--raw-logs`, and the mode flags (`--secondary`,
    `--multi-computer`, `--slurm`) to choose a prefix and verbosity. The
    full argparse pass happens later; this is just a fast lookahead.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--raw-logs", action="store_true")

    help_requested = "-h" in args_list or "--help" in args_list

    if not help_requested:
        early_args, _ = parser.parse_known_args(args_list)

        if "--secondary" in args_list:
            prefix = "S|"
        elif "--multi-computer" in args_list or "--slurm" in args_list:
            prefix = "P|"
        else:
            prefix = ""

        log_level = logging.DEBUG if early_args.debug else logging.INFO
        logger = logging.getLogger()
        logger.setLevel(log_level)

        if early_args.raw_logs:
            log_format = f"{prefix}%(message)s"
            logging.basicConfig(level=log_level, format=log_format)
        else:
            if prefix:
                log_format = f"%(levelname)s | %(asctime)s |{prefix}| %(message)s"
            else:
                log_format = "%(levelname)s | %(asctime)s | %(message)s"
            logging.basicConfig(level=log_level, format=log_format, datefmt="%H:%M:%S")
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s | %(asctime)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        logger = logging.getLogger()

    return logger
