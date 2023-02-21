import datetime
import mimetypes
import os.path, threading, logging
from urllib import parse

from flask import Flask, render_template, request, redirect, url_for
from flask_wtf import FlaskForm, CSRFProtect
from wtforms.validators import InputRequired
from wtforms.fields import *
from flask_bootstrap import Bootstrap5
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import time, timedelta, datetime
from marshmallow import fields
import requests
import ftplib

SETTINGS_FILE = 'settings.json'
IMG_PATH = "static/pull/"
IMG_LATEST_PATH = IMG_PATH + 'latest'
RETRIES = 3
TIMEOUT = 15
LOG_FILE = 'log.txt'

app = Flask(__name__)
app.secret_key = 'hahahahahah2142343'
app.debug = True
app.logger.setLevel(logging.INFO)

app.config['BOOTSTRAP_BTN_STYLE'] = 'primary'

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("imgpull")

bootstrap = Bootstrap5(app)
csrf = CSRFProtect(app)
job = None


class Job(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        self.stopped.set()
        self.join()

    def run(self):
        self.execute(*self.args, **self.kwargs)
        while not self.stopped.wait(self.interval.total_seconds()):
            self.execute(*self.args, **self.kwargs)


@dataclass_json
@dataclass
class Settings:
    url: str = "http://test.com"
    interval: int = 60
    active_from: time = field(
        metadata=config(
            encoder=time.isoformat,
            decoder=time.fromisoformat,
            mm_field=fields.Time(format='iso')
        ),
        default=time(6, 0)
    )
    active_to: time = field(
        metadata=config(
            encoder=time.isoformat,
            decoder=time.fromisoformat,
            mm_field=fields.Time(format='iso')
        ),
        default=time(23, 0)
    )
    ftp_uri: str = ""
    delete_after_upload: bool = False

    @classmethod
    def load(cls) -> 'Settings':
        if os.path.isfile(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f:
                return cls.from_json(f.read())
        else:
            return cls()

    def save(self):
        with open(SETTINGS_FILE, 'w') as f:
            f.write(self.to_json())


settings = Settings.load()


class PullForm(FlaskForm):
    url = URLField(description="Url to pull image from", validators=[InputRequired()])
    interval = IntegerField(description="Pull interval in minutes", validators=[InputRequired()])
    active_from = TimeField(description="Capture start time", validators=[InputRequired()])
    active_to = TimeField(description="Capture end time", validators=[InputRequired()])
    ftp_uri = URLField(description="FTP server to upload files to")
    delete_after_upload = BooleanField(description="Should the photos be deleted after successful upload?")
    submit = SubmitField()


def upload_ftp(host, username, password, filename, remote_folder="", port=21):
    session = ftplib.FTP_TLS()
    session.connect(host, port)
    session.login(username, password)
    with open(filename, 'rb') as f:  # file to send
        session.storbinary(f'STOR {os.path.join(remote_folder, os.path.basename(filename))}', f)
    session.quit()


def pull():
    cur_time = datetime.utcnow().time()
    if cur_time < settings.active_from or cur_time > settings.active_to:
        log.info(f"Sleeping...")
        return

    log.info(f"Pulling image from {settings.url}")

    for t in range(RETRIES):
        filename = f"{IMG_PATH}{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        try:
            response = requests.get(settings.url, stream=True, timeout=TIMEOUT)

            if not response.ok:
                log.error(response)
                break

            filename += mimetypes.guess_extension(response.headers['content-type'])

            log.info(f"Saving image as {filename}")

            with open(filename, 'wb') as handle:

                for block in response.iter_content(1024):
                    if not block:
                        break

                    handle.write(block)

            if os.path.exists(IMG_LATEST_PATH) or os.path.islink(IMG_LATEST_PATH):
                os.remove(IMG_LATEST_PATH)

            log.info("Creating symlink")
            os.symlink(os.path.abspath(filename), IMG_LATEST_PATH)

            if settings.ftp_uri:
                uri = parse.urlparse(settings.ftp_uri, allow_fragments=True)
                if uri.scheme != 'ftp':
                    log.error(f"Not a ftp uri: {settings.ftp_uri}")
                    break

                log.info("Uploading file to ftp")
                upload_ftp(uri.hostname,
                           uri.username,
                           uri.password,
                           filename,
                           port=uri.port if uri.port else 21)

                if settings.delete_after_upload:
                    log.info("Deleting")
                    os.rename(filename, IMG_LATEST_PATH)

            log.info("Done!")
            break
        except Exception as ex:
            if os.path.isfile(filename):
                os.remove(filename)

            log.error(f"Failed to pull image (try {t+1}/{RETRIES})", exc_info=ex)


def init_jobs():
    global job
    if job:
        job.stop()
    job = Job(interval=timedelta(minutes=int(settings.interval)), execute=pull)
    job.start()


@app.route('/', methods=["GET", "POST"])
def home():
    global settings
    form = PullForm(obj=settings)
    if form.validate_on_submit():
        settings = Settings.from_dict(request.form)
        settings.save()
        init_jobs()
        log.info(f"Settings updated: {settings}")
        return redirect(url_for('home'))

    with open(LOG_FILE, 'r') as f:
        return render_template(
            'pull.html',
            form=form,
            log=f.read()
        )


if __name__ == '__main__':
    if (app.debug and os.environ.get("WERKZEUG_RUN_MAIN") == "true") or not app.debug:
        init_jobs()

    try:
        app.run()
    except SystemExit:
        print("Program killed: stopping jobs")
        if job:
            job.stop()
