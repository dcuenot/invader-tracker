import datetime
import json
import logging
import os
import uuid
from json import JSONEncoder
from pprint import pprint
from typing import List, Set, Dict

import azure.functions as func
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from azure.storage.blob import BlobClient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Player:

    def __init__(self, name: str, score: int, invaders_count: int, rank: int, city_count: int):
        self.__name = str(name)
        self.__score = score
        self.__invaders_count = invaders_count
        self.__rank = rank
        self.__city_count = city_count

    @property
    def name(self):
        return self.__name

    @property
    def slack_name(self):
        return ''.join(e for e in self.__name.lower() if e.isalnum())

    @property
    def score(self):
        return self.__score

    @property
    def invaders_count(self):
        return self.__invaders_count

    @property
    def rank(self):
        return self.__rank

    @property
    def city_count(self):
        return self.__city_count

    @classmethod
    def of(cls, json: Dict):
        return cls(json.get('name'), json.get('score'), json.get('invaders_count'),
                   json.get('rank'), json.get('city_count'))

    def __repr__(self):
        return f'{self.__rank}. {self.__name} {self.__score} pts ({self.__invaders_count})'

    def __eq__(self, other):
        if (self.__name == other.name) & (self.__score == other.score) & (
                self.__invaders_count == other.invaders_count):
            return True
        else:
            return False

    def __hash__(self):
        return hash((self.__name, self.__score, self.__invaders_count))


class PlayerEncoder(JSONEncoder):
    def default(self, o):
        return {
            "name": o.name,
            "score": o.score,
            "invaders_count": o.invaders_count,
            "rank": o.rank,
            "city_count": o.city_count
        }


class PlayerDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=PlayerDecoder.object_hook, *args, **kwargs)

    @staticmethod
    def object_hook(obj):
        return Player.of(obj)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logger.info('The timer is past due!')

    logger.info('Python timer trigger function ran at %s', utc_timestamp)
    plop()


def plop() -> None:
    logger.info('call top 50')
    current_top50 = get_list_top_50()

    client = WebClient(token=os.getenv('SLACK_TOKEN'))

    try:
        current_path = __read_file('CURRENT.txt')
        current_str = __read_file(f'{current_path}.json')
        old_players = json.loads(current_str, cls=PlayerDecoder)
        diff = compute_diff(current_top50, old_players)

        if len(diff) > 0:
            last_flashes = get_last_flashes()

            url = f"https://api.telegram.org/{os.getenv('TELEGRAM_API')}/sendMessage"
            for line in diff:
                # Tests
                _ = requests.get(url, params={'chat_id': -427728024, 'text': line.get('msg')}, timeout=10)
                # Official
                #_ = requests.get(url, params={'chat_id': -477216106, 'text': line.get('msg')}, timeout=10)
                client.chat_postMessage(channel='#general', text=line.get('msg'))
                response = client.chat_postMessage(channel=line.get('player').slack_name, text=line.get('msg'))

                potentials = filter_potential_flash(line.get('player'), last_flashes)
                for potential in potentials:
                    create_slack_channel(client, line.get('player'))
                    client.chat_postMessage(
                        channel=line.get('player').slack_name,
                        thread_ts=response.get('ts'),
                        text=f"{potential.get('player')} - {potential.get('city')} - <http://space-invaders.com{potential.get('img')}|lien>",
                    )

            persist_top_50_and_last_flashes(current_top50, last_flashes)

    except Exception as exception:
        # first call in new blob storage
        logger.error(exception)
        last_flashes = get_last_flashes()
        persist_top_50_and_last_flashes(current_top50, last_flashes)
        for player in current_top50:
            logger.info(player.name.lower())
            create_slack_channel(client, player)


def filter_potential_flash(player: Player, last_flashes):
    res_flasher = []
    res_anonymous_paris = []
    res_anonymous_other = []

    server_time = datetime.datetime.fromtimestamp(last_flashes.get('timestamp'))
    for flash in last_flashes.get('with_paris'):
        flash_time = datetime.datetime.fromtimestamp(flash.get('timestamp'))
        if (server_time - flash_time).total_seconds() < 660:
            if flash.get('player') == player.name:
                res_flasher.append(flash)
            elif flash.get('player') == 'ANONYMOUS' and flash.get('city') == 'Paris':
                res_anonymous_paris.append(flash)
            elif flash.get('player') == 'ANONYMOUS':
                res_anonymous_other.append(flash)

    return res_flasher + res_anonymous_other + res_anonymous_paris


def create_slack_channel(client, player):
    try:
        client.conversations_create(name=player.slack_name)
    except SlackApiError as e:
        if e.response['error'] != 'name_taken':
            raise e


def object_hook(dct):
    return Player.of(dct)


def persist_top_50_and_last_flashes(current_top50, last_flashes):
    path = compute_path_from_timestamp(last_flashes.get('timestamp'))
    __persist_file(f'{path}.json', json.dumps(current_top50, cls=PlayerEncoder))
    __persist_file('CURRENT.txt', f'{path}')
    __persist_file(f'{path}.flashes', json.dumps(last_flashes.get('with_paris')))


def get_last_flashes():
    resp = __api_call(f'http://space-invaders.com/flashinvaders/flashes/', 'last_flash.json')
    return json.loads(resp)


def compute_path_from_timestamp(timestamp):
    logger.debug('extract timestamp')
    server_time = datetime.datetime.fromtimestamp(timestamp)
    path = server_time.strftime("%Y/%m/%Y-%m-%d %H:%M:%S")
    return path


def compute_diff(new, old):
    new = set(new)
    res = []

    for item in new:
        if item not in old:
            lookup = lookup_player(old, item.name, item.rank)
            if lookup.score != item.score:
                res.append({
                    'player': item,
                    'msg': f'{item.rank}. {item.name} a flashé {item.invaders_count - lookup.invaders_count} 👾 '
                           f'de {item.score - lookup.score} pts'
                })
            else:
                res.append({
                    'player': item,
                    'msg': f'{item.rank}. {lookup.name} s\'appelle maintenant {item.name}'
                })
    return res


def lookup_player(new_players: Set[Player], name, rank):
    for p in new_players:
        if p.name == name:
            return p
    for p in new_players:
        if p.rank == rank and rank != 50:
            return p

    return None


def get_list_top_50() -> List[Player]:
    uid = str(uuid.uuid4())
    resp = __api_call(f'http://space-invaders.com/api/highscore/?uid={uid}', 'highscore.json')

    res = []
    for row in json.loads(resp).get('Players'):
        if row.get('rank') < 51:
            res.append(Player.of(row))

    return res


def __api_call(url: str, local_file_name: str) -> str:
    logger.info('API call: %s', url)
    resp = None
    if os.environ.get('env', '') == 'local':
        try:
            resp = __read_file(local_file_name)
        except FileNotFoundError as exception:
            logger.error(exception)

    if resp is None:
        session = requests.Session()
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        r = session.get(url)

        if r.status_code != 200:
            raise ConnectionError

        resp = r.text
        __persist_file(local_file_name, resp)

    return resp


def __persist_file(path: str, content: str):
    logger.info('persist file: %s', path)
    if os.environ.get('env', '') == 'local':
        path = 'files/' + path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        f = open(path, "w")
        f.write(content)
        f.close()
    else:
        service = BlobClient.from_connection_string(
            os.getenv('AzureWebJobsStorage'),
            container_name='history',
            blob_name=path
        )

        try:
            service.upload_blob(content, overwrite=True)
        except Exception as exception:
            logger.error(exception)


def __read_file(path: str) -> str:
    logger.info('read file: %s', path)
    if os.environ.get('env', '') == 'local':
        f = open('files/' + path, "r")
        return f.read()
    else:
        service = BlobClient.from_connection_string(
            os.getenv('AzureWebJobsStorage'),
            container_name='history',
            blob_name=path
        )

        try:
            return service.download_blob().readall().decode("utf-8")
        except Exception as exception:
            logger.error(exception)


if __name__ == "__main__":
    print()
    logger.info('==> START')

    plop()

    logger.info('==> END')