from __future__ import annotations

import logging


class ContextLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        context = " ".join(f"{k}={v}" for k, v in self.extra.items() if v is not None)
        if context:
            msg = f"{msg} | {context}"
        return msg, kwargs



def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )



def get_logger(name: str, **context: object) -> ContextLoggerAdapter:
    logger = logging.getLogger(name)
    return ContextLoggerAdapter(logger, context)
