import logging
import sys


eodms_logger = logging.getLogger("eodms_dds")
eodms_logger.addHandler(logging.StreamHandler(sys.stdout))
eodms_logger.setLevel(logging.INFO)


class _EODMSLogger(logging.LoggerAdapter):
    def __init__(self, header: str, logger: logging.Logger):
        super().__init__(logger, {})
        self.header = header

    def process(self, msg: str, kwargs):
        # Apply header to message
        return f"| {self.header} | {msg}", kwargs
