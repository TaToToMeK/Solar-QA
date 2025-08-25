import logging

def setup_custom_levels():
    LEVELS = {
        "TRACE": 5,
        "VERBOSE": 15,
        "NOTICE": 25,
        "ALERT": 45,
    }
    for name, level in LEVELS.items():
        logging.addLevelName(level, name)

        def log_for_level(self, message, *args, _level=level, **kwargs):
            if self.isEnabledFor(_level):
                self._log(_level, message, args, **kwargs)

        setattr(logging.Logger, name.lower(), log_for_level)

setup_custom_levels()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s |%(lineno)4d:%(filename)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.trace("To jest TRACE")
    logger.verbose("To jest VERBOSE")
    logger.notice("To jest NOTICE")
    logger.alert("To jest ALERT")
    logger.info("Normalne INFO")
