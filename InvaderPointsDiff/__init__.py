import datetime
import json
import logging
import uuid
from json import JSONEncoder
from pprint import pprint
from typing import List, Set, Dict

import azure.functions as func
import requests
from azure.storage.blob import BlobClient


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
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    plop()


def plop() -> None:
    # current_top50 = get_list_top_50()

    # # Get data from Website
    # resp = requests.get('http://space-invaders.com/flashinvaders/flashes/')
    # content = resp.json()
    # # pprint(content.get('with_paris'))
    # #
    # # print(content.get('timestamp'))
    # server_time = datetime.datetime.fromtimestamp(content.get('timestamp'))
    # print(server_time.isoformat())
    # path = server_time.strftime("%Y/%m/%Y-%m-%d %H:%M:%S")
    # print(path)

    # messages = []
    # for flash in content['with_paris']:
    #     flash['flashed_at'] = datetime.datetime.fromtimestamp(flash['timestamp']).isoformat()
    #     if PROVIDER.select(flash):
    #         logging.info('Flash: %s already stored (%s) at: %s', flash['text'], flash['img'], flash['timestamp'])
    #     else:
    #         flash['path'] = __define_path_from_image(flash)
    #         logging.info(json.dumps(flash))
    #
    #         messages.append(Message(json.dumps(flash)))

    print('call top 50')
    current_top50 = get_list_top_50()

    try:
        current_path = __read_file('CURRENT.txt')
        current_str = __read_file(f'{current_path}.json')
        old_players = json.loads(current_str, cls=PlayerDecoder)
        diff = compute_diff(current_top50, old_players)


        last_flashes_str = __read_file(f'{current_path}.flashes')
        last_flashes = json.loads(last_flashes_str)

        print(current_path)
        pprint(last_flashes[0])

        print(diff)

        if len(diff) > 0:
            url = f"https://api.telegram.org/bot831369672:AAERDq4zQ0yaBjyMp-wtHdo8p3hsgikFCNg/sendMessage"
            for line in diff:
                # Tests
                _ = requests.get(url, params={'chat_id': -427728024, 'text': line.get('msg')}, timeout=10)
                # Official
                #_ = requests.get(url, params={'chat_id': -477216106, 'text': line.get('msg')}, timeout=10)

            persist_top_50_and_last_flashes(current_top50)

    except Exception as exception:
        # first call in new blob storage
        logging.error(exception)
        persist_top_50_and_last_flashes(current_top50)


def object_hook(dct):
    return Player.of(dct)


def persist_top_50_and_last_flashes(current_top50):
    content, path = info_from_flashes_api()
    print('persist file current_top50')
    __persist_file(f'{path}.json', json.dumps(current_top50, cls=PlayerEncoder))
    print('persist file CURRENT.txt')
    __persist_file('CURRENT.txt', f'{path}')
    print('persist file flashes')
    __persist_file(f'{path}.flashes', json.dumps(content.get('with_paris')))


def info_from_flashes_api():
    print('get flashes')
    resp = requests.get('http://space-invaders.com/flashinvaders/flashes/')
    content = resp.json()
    print('extract timestamp')
    server_time = datetime.datetime.fromtimestamp(content.get('timestamp'))
    path = server_time.strftime("%Y/%m/%Y-%m-%d %H:%M:%S")
    return content, path


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
        if p.rank == rank:
            return p

    return None


def get_list_top_50() -> List[Player]:
    uid = str(uuid.uuid4())
    resp = requests.get(f'http://space-invaders.com/api/highscore/?uid={uid}')

    if resp.status_code != 200:
        raise ConnectionError

    res = []
    for row in resp.json().get('Players'):
        if row.get('rank') < 51:
            res.append(Player.of(row))

    return res


def __persist_file(path: str, content: str):
    service = BlobClient.from_connection_string(
        'DefaultEndpointsProtocol=https;AccountName=flashinvaders;AccountKey=cKxGbc5erhNtMGPTgecoPP6Mee6WSgSR8JUoOIrkoG5ayt1ZTmI7pAcTmcxKgnGCMqU2CJaGHR3paKVZkPHXHQ==;EndpointSuffix=core.windows.net',
        container_name='history',
        blob_name=path
    )

    try:
        service.upload_blob(content, overwrite=True)
    except Exception as exception:
        logging.error(exception)


def __read_file(path: str) -> str:
    service = BlobClient.from_connection_string(
        'DefaultEndpointsProtocol=https;AccountName=flashinvaders;AccountKey=cKxGbc5erhNtMGPTgecoPP6Mee6WSgSR8JUoOIrkoG5ayt1ZTmI7pAcTmcxKgnGCMqU2CJaGHR3paKVZkPHXHQ==;EndpointSuffix=core.windows.net',
        container_name='history',
        blob_name=path
    )

    try:
        return service.download_blob().readall().decode("utf-8")
    except Exception as exception:
        logging.error(exception)


if __name__ == "__main__":
    print()
    print('START')

    plop()
