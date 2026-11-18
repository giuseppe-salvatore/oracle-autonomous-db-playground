"""Common reporting for command-line failures."""

import sys

from lib.utils.logger import log


def install_error_handler(command):
    def report_error(error_type, error, traceback):
        # Avoid logging exception text, which can contain connection details.
        log.error(
            "event=command_failed command=%s error_type=%s",
            command,
            error_type.__name__,
        )

    sys.excepthook = report_error
