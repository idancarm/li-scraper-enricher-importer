import json
import os
import time
import random
from pathlib import Path

from dotenv import load_dotenv


def init_env():
    env_path = Path('.env')
    if not env_path.exists():
        print('Missing .env file. Copy .env.example to .env and fill in your keys.')
        raise SystemExit(1)
    load_dotenv(env_path)


def sleep_ms(ms):
    time.sleep(ms / 1000)


def random_delay(min_ms, max_ms):
    time.sleep(random.uniform(min_ms, max_ms) / 1000)


def read_json(filepath):
    p = Path(filepath)
    if not p.exists():
        return []
    return json.loads(p.read_text())


def write_json(filepath, data):
    Path(filepath).write_text(json.dumps(data, indent=2))
