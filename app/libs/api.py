import aiohttp
import asyncio
from asyncio import Lock
from libs.utils import rate_limit, repeat_when_429_or_5xx
from json import dump, load, loads, dumps
from pathlib import Path
from uuid import uuid4
import time
from anyio import open_file
from libs.logs import EXEC_ID, request_logger
from sys import stdout
from hashlib import sha256
from traceback import format_exc
from libs.dirs import REQUEST_DIR
import re
from urllib.parse import unquote, urlparse


REQUEST_DIR = REQUEST_DIR / str(EXEC_ID)
if not REQUEST_DIR.is_dir():
    REQUEST_DIR.mkdir()

_request_counter = {
    "planned": 0,
    "started": 0,
    "ended": 0,

    "max_active_requests": 10,
    "stop": False
}


class BaseApi():
    r_lock = asyncio.Lock()

    def __init__(
        self, base_url, token,
        show_tooltips=True
    ):
        self.token = token
        self.base_url = base_url
        self.show_tooltips = show_tooltips

        self.local_cache = {}

    # utils
    def dump_cache(self, file_name):
        with open(file_name, 'w') as json_file:
            dump(self.local_cache, json_file, indent=4)

    def load_cache(self, file_name):
        if Path(file_name).is_file():
            with open(file_name, 'r') as json_file:
                self.local_cache = load(json_file)

    @property
    def headers(self):
        return {}

    @property
    def is_json(self):
        return {
            "Content-Type": "application/json"
        }

    # base request
    # @rate_limit(0.1, "global_requests", asyncio.Lock())
    @repeat_when_429_or_5xx
    async def _request(self, method, url, headers=None, json=None, params=None):
        if headers is None:
            headers = self.headers
        if json is not None:
            headers = {**headers, **self.is_json}

        _id = uuid4()

        _request_counter["started"] += 1
        async with aiohttp.ClientSession() as session:
            start = time.monotonic()
            async with session.request(
                method, f"{self.base_url}{url}", json=json, headers=headers, params=params
            ) as response:
                duration = round(time.monotonic()-start, 3)
                if duration > 35:
                    _request_counter["stop"] = True
                async with await open_file(REQUEST_DIR / f"{_id}.req", "w") as file:
                    await file.write(await response.text())
                if self.show_tooltips:

                    request_logger.info(f"{method} {self.base_url}{url} {response.status} {duration} {_id}")
                content_type = response.headers.get("Content-Type")
                if response.status != 200:
                    if response.status != 429:
                        request_logger.error(f"{await response.text()}")
                # if response.status == 404:
                #     return ({}, response.status)
                # if (response.status >= 500 or response.status == 429) and repeat <= 5:
                #     return await self._request(
                #         method, url, headers, json, params, repeat+1
                #     )
                if "application/json" in content_type:
                    _request_counter["ended"] += 1
                    return (await response.json(), response.status)
                else:
                    _request_counter["ended"] += 1
                    return (await response.text(), response.status)
        return ({}, 500)


class RapidApi(BaseApi):

    @property
    def headers(self):
        return self.token

    # 10 requests per second
    _request = rate_limit(0.1, "rapid", asyncio.Lock(), _request_counter)(BaseApi._request)

    async def info(self, user, id_=None):
        # v1
        answer = await self._request(
            "get", "info", params={"username_or_id_or_url": user}
        )
        if answer[1] == 404 or answer[1] == 403:
            return {"answer": {}, "id_": id_}
        return {"answer": answer[0], "id_": id_}

    async def highlights(self, user, id_=None):
        # v1
        answer = await self._request(
            "get", "highlights", params={"username_or_id_or_url": user}
        )
        if answer[1] == 404 or answer[1] == 403:
            return {"answer": {}, "id_": id_}
        return {"answer": answer[0], "id_": id_}

    async def reels(self, user, id_=None, pagination_token=None):
        # v1.2
        params = {"username_or_id_or_url": user}
        if pagination_token is not None:
            params["pagination_token"] = pagination_token
        answer = await self._request(
            "get", "reels", params=params
        )
        if answer[1] == 404 or answer[1] == 403:
            return {"answer": {}, "id_": id_}
        return {"answer": answer[0], "id_": id_}

    async def posts(self, user, id_=None, pagination_token=None):
        # v1.2
        params = {"username_or_id_or_url": user}
        if pagination_token is not None:
            params["pagination_token"] = pagination_token
        answer = await self._request(
            "get", "posts", params=params
        )
        if answer[1] == 404 or answer[1] == 403:
            return {"answer": {}, "id_": id_}
        return {"answer": answer[0], "id_": id_}

    async def stories(self, user, id_=None, pagination_token=None):
        # v1.2
        params = {"username_or_id_or_url": user}
        if pagination_token is not None:
            params["pagination_token"] = pagination_token
        answer = await self._request(
            "get", "stories", params=params
        )
        if answer[1] == 404 or answer[1] == 403:
            return {"answer": {}, "id_": id_}
        return {"answer": answer[0], "id_": id_}

    async def get_n_page(self, user, model, page_number=7, id_=None, callback=None):
        request_logger.debug(f"RapidApi get_n_page {user}/{model}")
        func = getattr(self, model)
        request_logger.debug(f"RapidApi get_n_page {user}/{model} get 1 page")
        answer = await func(user, id_)

        answer = answer["answer"]
        if "data" in answer:
            status = "Active"
        else:
            status = "Potential Ban"

        update_status = None
        if callback is not None:
            callback = callback(status)
            update_status = await callback(id_)

        if "data" in answer and "items" in answer.get("data", {}):
            records = answer["data"]["items"]
            page = 1
            while answer.get("pagination_token", None) is not None and page < page_number:
                request_logger.debug(f"RapidApi get_n_page {user}/{model} get {page+1} page")
                answer = await func(user, id_, answer.get("pagination_token"))
                answer = answer["answer"]
                if "data" in answer and "items" in answer.get("data", {}):
                    records += answer["data"]["items"]
                    page += 1
            return {
                "answer": records, "id_": id_, "update_status": update_status,
                'status': status
            }
        else:
            return {
                "answer": [], "id_": id_, "note": "Potential Ban",
                "update_status": update_status, 'status': status
            }


class AirtableApi(BaseApi):
    lock = Lock()

    @property
    def headers(self):
        return {
            "Authorization": f"Bearer {self.token}"
        }

    # 5 requests per second
    _request = rate_limit(0.25, "airtable", asyncio.Lock(), _request_counter)(BaseApi._request)

    # meta info
    async def _bases(self):
        answer = await self._request("get", "meta/bases")
        return answer[0]

    async def bases(self):
        async with self.lock:
            if "bases" not in self.local_cache:
                data = await self._bases()
                self.local_cache["bases"] = {}
                for base in data["bases"]:
                    self.local_cache["bases"][base["name"]] = base["id"]
            if self.show_tooltips:
                stdout.write("Bases Map\n")
                for name, id_ in self.local_cache["bases"].items():
                    stdout.write(f"\t{name}: {id_}\n")
                stdout.write("End Bases Map\n")
        return self.local_cache["bases"]

    async def _shema(self, baseName):
        answer = await self._request(
            "get", f"meta/bases/{self.local_cache['bases'][baseName]}/tables"
        )
        return answer[0]

    async def shema(self, baseName):
        async with self.lock:
            if "shema" not in self.local_cache:
                data = await self._shema(baseName)
                self.local_cache["shema"] = {}
                for shema in data["tables"]:
                    self.local_cache["shema"][shema["name"]] = shema["id"]
                    self.local_cache[f"shema_{shema['name']}_fields"] = {}
                    self.local_cache[f"shema_{shema['name']}_views"] = {}
                    for field in shema["fields"]:
                        self.local_cache[f"shema_{shema['name']}_fields"][field["name"]] = field["id"]

                    for view in shema["views"]:
                        self.local_cache[f"shema_{shema['name']}_views"][view["name"]] = view["id"]

            if self.show_tooltips:
                stdout.write("Shema Map\n")
                for name, id_ in self.local_cache["shema"].items():
                    stdout.write(f"\t{name}: {id_}\n")
                    stdout.write("\tFields\n")
                    for f_name, f_id_ in self.local_cache[f"shema_{name}_fields"].items():
                        stdout.write(f"\t\t {f_name}: {f_id_}\n")
                    stdout.write("\tViews\n")
                    for v_name, v_id_ in self.local_cache[f"shema_{name}_views"].items():
                        stdout.write(f"\t\t {v_name}: {v_id_}\n")
                stdout.write("End Shema Map\n")

        return self.local_cache["shema"]

    async def init(self, baseName=None):
        # процесс получения данных связанных с структурой базы данных с
        # сервера,их вывод при show_tooltips=True,
        # и создание кеша с ними для дальнейшего использования
        await self.bases()
        if baseName is None:
            baseName = input("Enter Base Name: ")
        if baseName not in self.local_cache["bases"]:
            raise Exception("Bad Base Name")
        await self.shema(baseName)

    # user requests
    async def search(self, baseName, tableName, maxRecords=None, view=None, fields=None, filterByFormula=None, offset=None):
        base = self.local_cache['bases'][baseName]
        table = self.local_cache['shema'][tableName]
        params = {}
        if maxRecords is not None:
            params["maxRecords"] = maxRecords
        if view is not None:
            params["view"] = view
        if fields is not None:
            params["fields"] = fields
        if filterByFormula is not None:
            params["filterByFormula"] = filterByFormula
        if offset is not None:
            params["offset"] = offset
        request_logger.debug(f"Airtable search {baseName}/{tableName} with {params}")
        answer = await self._request("get", f"{base}/{table}", params=params)
        return answer[0]

    async def search_by_formula(self, base, table, formula):
        reel = await self.search(
            base, table, maxRecords=1, filterByFormula=formula
        )
        if len(reel["records"]) > 0:
            reel_id = reel["records"][0]["id"]
        else:
            reel_id = None
        return reel_id

    async def search_until(self, baseName, tableName, maxRecords=None, view=None, fields=None, filterByFormula=None):
        answer = await self.search(baseName, tableName, maxRecords, view, fields, filterByFormula)
        records = answer["records"]
        while "offset" in answer:
            answer = await self.search(baseName, tableName, maxRecords, view, fields, filterByFormula, answer["offset"])
            records += answer["records"]
        return {"records": records}

    async def update(self, baseName, tableName, recordId, fields):
        base = self.local_cache['bases'][baseName]
        table = self.local_cache['shema'][tableName]
        request_logger.debug(f"Airtable update {baseName}/{tableName}/{recordId} with {fields}")
        answer = await self._request("patch", f"{base}/{table}/{recordId}", json={
            "fields": fields
        })
        return answer[0]

    async def upsert(self, baseName, tableName, mergeFields, records):
        base = self.local_cache['bases'][baseName]
        table = self.local_cache['shema'][tableName]
        request_logger.debug(f"Airtable upsert {baseName}/{tableName}")

        data = {
            "performUpsert": {
                "fieldsToMergeOn": mergeFields
            },
            "records": records
        }
        if mergeFields is None:
            del data["performUpsert"]

        answer = await self._request("patch", f"{base}/{table}", json=data)
        return answer[0]

    async def create(self, baseName, tableName, fields):
        base = self.local_cache['bases'][baseName]
        table = self.local_cache['shema'][tableName]
        request_logger.debug(f"Airtable create {baseName}/{tableName} with {fields}")
        answer = await self._request("post", f"{base}/{table}", json={
            "fields": fields
        })
        return answer[0]

    async def get(self, baseName, tableName, recordId):
        base = self.local_cache['bases'][baseName]
        table = self.local_cache['shema'][tableName]
        request_logger.debug(f"Airtable get {baseName}/{tableName}/{recordId}")
        answer = await self._request("get", f"{base}/{table}/{recordId}")
        return answer[0]


class RapidTwitterApi(BaseApi):
    @property
    def headers(self):
        return self.token

    _request = rate_limit(0.1, "rapid", asyncio.Lock(), _request_counter)(BaseApi._request)

    async def followingids(self, username, count=50):
        data = {"username": username, 'count': count}
        answer = await self._request(
            "get", "FollowingIds", params=data
        )
        if answer[1] == 404 or answer[1] == 403:
            return None
        return answer[0]

    async def users(self, username):
        data = {"usernames": username}
        answer = await self._request(
            "get", "Users", params=data
        )
        if answer[1] == 404 or answer[1] == 403:
            return None
        return answer[0]

    async def userbyrestid(self, id_):
        data = {"id": id_}
        answer = await self._request(
            "get", "UserByRestId", params=data
        )
        if answer[1] == 404 or answer[1] == 403:
            return None
        return answer[0]


class RapidTikTokApi(BaseApi):

    @property
    def headers(self):
        return self.token

    _request = rate_limit(0.1, "rapid", asyncio.Lock(), _request_counter)(BaseApi._request)

    async def user_posts(self, user, pagination_token=None, count=20):
        data = {"user_id": user, 'count': count}
        if pagination_token:
            data['max_cursor'] = pagination_token
        answer = await self._request(
            "get", "user-posts", params=data
        )
        if answer[1] == 404 or answer[1] == 403:
            return None
        return answer[0]

    async def get_n_page(self, user, count=20, n=1):
        answer = {"aweme_list": []}
        page_1 = await self.user_posts(user, count=20)
        if 'aweme_list' in page_1 and page_1['aweme_list'] is not None:
            answer['aweme_list'] += page_1['aweme_list']
        c = 2
        mc = count // 20
        next_ = None
        if 'max_cursor' in page_1:
            next_ = page_1['max_cursor']
        while c <= mc and next_ is not None:
            page_1 = await self.user_posts(user, count=20, pagination_token=next_)
            if 'aweme_list' in page_1 and page_1['aweme_list'] is not None:
                answer['aweme_list'] += page_1['aweme_list']
            if 'max_cursor' in page_1:
                next_ = page_1['max_cursor']
            c += 1
        return answer

    async def get_user(self, username):
        username = username.lower()
        answer = await self._request(
            "get", "get-user", params={"username": username}
        )
        if answer[1] == 404 or answer[1] == 403:
            return None
        return answer[0]


class Loader():
    total_bytes = 0

    @rate_limit(0.2, "loader", asyncio.Lock(), _request_counter)
    async def load(self, url, name):
        print(f"Start download {url}")
        async with aiohttp.ClientSession() as session:
            async with session.request("get", url) as response:
                data = await response.read()
                print(f"End download {url}")
                with open(f"{name}.mp4", "bw") as f:
                    f.write(data)
                hash_ = sha256(data).hexdigest()
                del data
                return hash_

    @rate_limit(0.1, "simple_get", asyncio.Lock(), _request_counter)
    async def simple_get(self, url, id_=None):
        _id = uuid4()
        _request_counter['started'] += 1
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request("get", url) as response:
                    if response.status == 200:
                        text = await response.text()
                        # async with await open_file(REQUEST_DIR / f"{_id}.req", "w") as file:
                        #     await file.write(f'{url} - {id_}\n{text}')
                        _request_counter['ended'] += 1
                        return text
        except Exception:
            request_logger.error(f'{url} {id_}\n{format_exc()}')
        _request_counter['ended'] += 1
        return None

    @rate_limit(0.2, "loader", asyncio.Lock(), _request_counter)
    async def load_hash(self, url, r=0):
        print(f"Start download {url}")
        if r >= 4:
            return "UNDEFINED HASH"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request("get", url) as response:
                    data = await response.read()
                    print(f"End download {url}")
                    self.total_bytes += len(data)
                    hash_ = sha256(data).hexdigest()
                    del data
                    return hash_
        except Exception:
            request_logger.error(f"Download {url} filed!")
            request_logger.error(f"{format_exc()}")
            await asyncio.sleep(5)
            await self.load_hash(url, r + 1)


class LinkAnalizer:

    insecure_file_types = [
        '.js',
        '.pdf',
        '.mp3',
        '.mp4',
        '.mkv',
        '.jpeg',
        '.png',
        '.jpg',
        '.svg',
        '.css',
        '.less',
        '.sass',
        '.ttf',
        '.otf',
        '.woff',
        '.woff2',
        '.eot'
    ]

    bad_parts = [
        'google.', 'yahoo.', 'yandex.', 'spotify.', 'cloudflare.', 'gstatic', 'static', 'media', 'cdn.',
        'wp-content', 'cache', 'minify', 'share', 'img', 'www.w3.org', 'discord.', 'facebook.', 'twitter.'
    ]

    of_name = "\b[oO][nN][lL][yY][fF][aA][nN][sS]\b"

    if (of_urls_from_html.length == 0) {
  const of_word = $input.item.json.content.match(//g) || []
  if (of_word.length > 0) {
    $input.item.json.result.word_match = true
  }
}

    # a = параметр совпадения по поискам (5% попаданий из всей выборки)
    # b = минимальная выборка для принятия решения
    def __init__(self, state, loader, instagraapi, a=5, b=1000):
        self.state_path = Path(f'./{state}')
        self.state = self.load_state()
        self.loader = loader
        self.a = a
        self.b = b
        self.instagraapi = instagraapi

        self.rate = self.get_rate()

        self.filter_rules = self.get_rules()
        self.cache = {}
        self.hit_in_cache = 0
        self.detected_links = 0
        self.total_processed = 0

    def get_rules(self):
        def to_short_or_long(link):
            parts = link.replace('https://', '').replace('http://', '').split('/')
            if '' in parts:
                parts.remove('')
            return len(parts) == 1 or len(parts) > 3
        def insecure_types(link):
            if '&quot' in link:
                link = link.split('&quot')[0]
            parts = link.replace('https://', '').replace('http://', '').split('?')[0]
            parts = parts.split('/')
            return any(list(map(lambda f: f in parts[-1], self.insecure_file_types)))
        def bad_parts(link):
            if '&quot' in link:
                link = link.split('&quot')[0]
            return any(list(map(lambda f: f in link, self.bad_parts)))

        return [to_short_or_long, insecure_types, bad_parts]

    def sort_links(self, links):
        links = list(map(lambda link: [link, self.get_domain(link)], links))
        links.sort(key=lambda link: self.rate.index(link[1]) if link[1] in self.rate else 1000000)

        for link in links:
            if any(list(map(lambda f: f(link[0]), self.filter_rules))):
                request_logger.info(f"skip this link: {link}")

        return [link[0] for link in links if not any(list(map(lambda f: f(link[0]), self.filter_rules)))]

    def get_rate(self):
        rate = [{'domain': k, **v} for k, v in self.state.items() if k != '_patterns']
        rate.sort(key=lambda x: x.get('%', 0))
        return list(map(lambda x: x['domain'], rate))

    def load_state(self):
        try:
            return loads(self.state_path.read_text())
        except Exception:
            return {
                '_patterns': [
                    'https?:\/\/(?:www\.)?onlyfans\.com\/[a-zA-Z0-9_\-\.]+',
                    'https?:\/\/(?:www\.)?[a-zA-Z0-9\-]+\.[a-zA-Z]+(?:\/[a-zA-Z0-9\/\.\?&=+*\-#$%\^\(\)\[\]\{\}\+-:;,!]*)?'
                ]
            }

    def get_domain(self, link):
        return link.replace(
            'https://', ''
        ).replace('http://', '').split('/')[0]

    def check(self, link):
        domain = self.get_domain(link)
        if domain not in self.state:
            self.state[domain] = {
                'verified': True,
                'success': 0,
                '%': 0,
                'all': 0
            }
            return True, domain
        verified = self.state[domain]['verified']
        has_success = self.state[domain]['all'] < self.b or self.state[domain]['success'] / (self.state[domain]['all'] / 100) > self.a
        return (verified and has_success) or self.state[domain].get('ignore_all_rules', False), domain

    async def get_data(self, link):
        data = await self.loader.simple_get(link)
        return data

    def find(self, data):
        of = False
        answer = re.findall(self.state['_patterns'][0], data, flags=re.IGNORECASE)
        if not answer:
            answer = re.findall(self.state['_patterns'][1], data, flags=re.IGNORECASE)
        else:
            of = True
        return answer, of

    async def analize(self, link, depth=0):
        send = False
        if depth == 0:
            self.total_processed += 1
        if link in self.cache:
            self.hit_in_cache += 1
            return self.cache[link]
        answer, of = self.find(link)
        if of:
            return answer, True, send
        request_logger.info(f"scan this link: {link}")
        if depth == 3:
            if link not in self.cache:
                self.cache[link] = ([], False, send)
            return [], False, send
        check, domain = self.check(link)
        if check:
            self.state[domain]['all'] += 1
            if domain == 'instagram.com':
                try:
                    username = link.split('?')[0].split('/')[-1]
                    data = (await self.instagraapi.info(username))
                    url = data.get('answer', {}).get('data', {}).get('external_url', '')
                    if url != '':
                        answer, iisof, insend = await self.analize([url], depth + 1)
                        send |= insend
                        if link not in self.cache:
                            self.cache[link] = answer
                        if iisof:
                            self.detected_links += 1
                        return answer, iisof, send
                    if link not in self.cache:
                        self.cache[link] = ([], False)
                    return [], False, send
                except Exception:
                    if link not in self.cache:
                        self.cache[link] = ([], False)
                    return [], False, send
            data = await self.get_data(link)
            if re.findall(self.of_name, data, flags=re.IGNORECASE):
                send |= True

            if not data:
                if link not in self.cache:
                    self.cache[link] = ([], False)
                return [], False, send
            answer, of = self.find(unquote(data))
            if of:
                self.state[domain]['success'] += 1
                if link not in self.cache:
                    self.detected_links += 1
                    self.cache[link] = (answer, True)
                return answer, True, send

            for dlink in self.sort_links(answer):
                request_logger.info(f"scan this link: {dlink}")
                scheck, sdomain = self.check(dlink)
                if scheck:
                    answer, isof, nsend = await self.analize(dlink, depth + 1)
                    send |= nsend
                    if depth == 0 and isof:
                        self.state[domain]['success'] += 1
                    if isof:
                        self.detected_links += 1
                        if link not in self.cache:
                            self.cache[link] = (answer, True, send)
                        return answer, True, send
        if link not in self.cache:
            self.cache[link] = ([], False, send)
        return [], False, send

    def update_state(self):
        for domain in self.state.keys():
            if domain != '_patterns':
                d = self.state[domain]['all'] / 100
                if d == 0:
                    self.state[domain]['%'] = 0
                else:
                    self.state[domain]['%'] = self.state[domain]['success'] / d
        self.state_path.write_text(dumps(self.state, indent=4))
