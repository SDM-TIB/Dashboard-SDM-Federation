
import logging
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    fileHandler = logging.FileHandler("{0}.log".format('fedsdm'))
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)


if __name__ == "__main__":
    from fedsdm import create_app
    app = create_app()
    app.run(port=5000, host="0.0.0.0")
