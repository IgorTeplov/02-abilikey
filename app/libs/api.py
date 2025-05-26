import aiohttp
import asyncio
from asyncio import Lock
from libs.utils import rate_limit, repeat_when_429_or_5xx
from json import dump, load
from pathlib import Path
from uuid import uuid4
import time
from anyio import open_file
from libs.logs import EXEC_ID, request_logger, BASE_DIR
from sys import stdout
from requests import post
from hashlib import sha256
from traceback import format_exc


REQUEST_DIR = BASE_DIR / 'requests'
if not REQUEST_DIR.is_dir():
    REQUEST_DIR.mkdir()

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
                with open(f"test/{name}.mp4", "bw") as f:
                    f.write(data)
                return sha256(data).hexdigest()

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
                    return sha256(data).hexdigest()
        except Exception:
            request_logger.error(f"Download {url} filed!")
            request_logger.error(f"{format_exc()}")
            await asyncio.sleep(5)
            await self.load_hash(url, r + 1)
