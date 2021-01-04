import logging
import sys
import traceback

from flask import Flask, jsonify

from pesto_common.log.logger_factory import LoggerFactory

from router.example import app_example

logger = LoggerFactory.get_logger('main')

log = logging.getLogger('werkzeug')
log.setLevel(logging.WARNING)

app = Flask(__name__)

app.register_blueprint(app_example)


@app.route('/')
def index():
    data = {'name': 'pesto-example'}
    return jsonify(data)


if __name__ == '__main__':
    port = 8080
    try:
        app.run(host='0.0.0.0', port=port)
    except (KeyboardInterrupt, SystemExit):
        print('')
        logger.info('Program exited.')
    except (Exception,):
        logger.error('Program exited. error info:\n')
        logger.error(traceback.format_exc())
        sys.exit(0)
