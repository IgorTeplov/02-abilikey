import asyncio
import libs.init

from libs.api import AirtableApi, RapidApi, Loader, _request_counter
from libs.settings import AIRTABLE_TOKEN, XRapidAPIHost, XRapidAPIKey
from libs.utils import chunks
from datetime import datetime
from libs.logs import execution_logger, error_logger
from libs.makepipeline import Step, Const, OUT
from cache.models import Sound
from traceback import format_exc


const = Const(
    BASE='Instagram',
    ACCOUNTS='IG Accounts',
    MODEL='IG Reels',
    SOUNDS='Sounds',
    URL_MODEL='reels',
    ACCOUNTS_VIEW="Reels - Duplicate Test",
    PAGES=14,
    USER_COUNT=3,
    FIELDS=["Username", "Created", "Account ID", "Model"]
)

airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN
)
rapid = RapidApi(
    "https://instagram-scraper-20252.p.rapidapi.com/v1/",
    {"X-RapidAPI-Key": XRapidAPIKey, "X-RapidAPI-Host": XRapidAPIHost}
)

L = Loader()

if __name__ == '__main__':

    def get_sound(model):
        sound = None
        if model.get("clips_metadata", {}).get("audio_type", "") == "licensed_music":
            _data = model.get("clips_metadata", {}).get("music_info", {}).get("music_asset_info", None)
            if _data is not None:
                sound = {
                    "Sound ID": str(_data["audio_id"]),
                    "Sound Title": str(_data["title"]),
                    "Sound Type": "Licensed Music"
                }
        else:
            _data = model.get("clips_metadata", {}).get("original_sound_info", None)
            if _data is not None:
                sound = {
                    "Sound ID": str(_data["audio_id"]),
                    "Sound Title": str(_data["original_audio_title"]),
                    "Sound Type": "Original Sound"
                }
        return sound

    async def get_hash(model):
        hash_ = await L.load_hash(model['video_url'])
        model['hash'] = hash_
        return model

    async def _step1(data: None, context, const, pipe, iteration, step_number) -> None:
        airtabel.load_cache("airtable_db.json")
        await airtabel.init(const.BASE)
        airtabel.dump_cache("airtable_db.json")

    async def _step2(data: None, context, const, pipe, iteration, step_number) -> dict:
        if const.USER_COUNT is not None:
            users = await airtabel.search(
                const.BASE, const.ACCOUNTS, const.USER_COUNT,
                const.ACCOUNTS_VIEW, const.FIELDS
            )
        else:
            users = await airtabel.search_until(
                const.BASE, const.ACCOUNTS, None, const.ACCOUNTS_VIEW,
                const.FIELDS
            )
        return users

    async def _step3(data: dict, context, const, pipe, iteration, step_number) -> list:
        username = data['fields']['Username']
        model = data['fields']['Model']
        answer = await rapid.get_n_page(username, const.URL_MODEL, const.PAGES)
        models = answer.get('answer', [])
        models.sort(key=lambda item: -item['taken_at'])
        return {'models': models, 'user': data, 'model': model, 'status': answer.get('status')}

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> list:
        if 'models' not in pipe:
            pipe['models'] = {}

        if data['model'][0] not in pipe['models']:
            pipe['models'][data['model'][0]] = {
                'users': [],
                'reels': []
            }
        pipe['models'][data['model'][0]]['users'].append(data['user'])

        new_reels = []
        for reel in data['models']:
            sound_data = get_sound(reel)
            if sound_data is None:
                continue
            new_reel = {
                "username": data['user'],
                "video_id": reel["id"],
                "video_duration": reel["video_duration"],
                "video_url": reel["video_url"],
                "code": reel["code"],
                "taken_at": reel["taken_at"],
                "sound_data": sound_data,
            }
            new_reels.append(new_reel)
        tasks = []
        for reel in new_reels:
            tasks.append(get_hash(reel))
        new_reels = await asyncio.gather(*tasks)
        new_reels.sort(key=lambda item: -item['taken_at'])
        pipe['models'][data['model'][0]]['reels'] += new_reels

    async def _step5(data: list, context, const, pipe, iteration, step_number) -> list:
        return pipe

    async def _step6(data: dict, context, const, pipe, iteration, step_number) -> list:
        sound_data = await get_sound(data, const)
        update = {
            "Account": [data['_user']["id"]],
            "IG Reel URL": f"https://www.instagram.com/reel/{data['code']}",
            "Reel ID": str(data["id"]),
            "Views": data["play_count"],
            "Likes": data["like_count"],
            "Comments": data["comment_count"],
            "Posting Time": data["taken_at"] * 1000,
            "Reel Duration": data.get("video_duration", 0),
            "Posting Date": datetime.fromtimestamp(
                data["taken_at"]
            ).strftime("%Y-%m-%d"),
        }
        if '_diff' in data:
            update["Difference (Hours) to previous Post"] = data['_diff']
        if "caption" in data:
            if isinstance(data["caption"], dict) and "text" in data["caption"]:
                update["Caption"] = data["caption"]["text"]

        if data.get("location", None):
            update["Location - Name"] = data["location"]["name"]
            update["Location ID"] = data["location"]["id"]

        if sound_data:
            update["Sounds"] = [sound_data.airtabel_id]
            update["Sound"] = sound_data.rapid_id

        return {
            'fields': update
        }

    async def _step7(data: list, context, const, pipe, iteration, step_number) -> list:
        return list(chunks(data, 10))

    async def _step8(data: dict, context, const, pipe, iteration, step_number) -> dict:
        answer = await airtabel.upsert(
            const.BASE, const.MODEL, ['Reel ID'], data
        )
        if 'error' in answer:
            error_logger.error(f"Bad account upsert: {data}, error: {answer}")
        return answer

    async def _step9(data: dict, context, const, pipe, iteration, step_number) -> list:
        data = []
        for item in context[3]['out']:
            data.append({
                'id': item['user']['id'],
                'fields': {
                    "Status": item['status'],
                    "Reels - Last Scraped": datetime.today().strftime("%Y-%m-%d")
                }
            })
        return list(chunks(data, 10))

    async def _step10(data: dict, context, const, pipe, iteration, step_number) -> dict:
        answer = await airtabel.upsert(
            const.BASE, const.ACCOUNTS, None, data
        )
        if 'error' in answer:
            error_logger.error(f"Bad account upsert: {data}, error: {answer}")
        return answer

    step1 = Step(_step1, name='step_1', const=const)
    step2 = Step(_step2, name='step_2', const=const)
    step3 = Step(_step3, name='step_3', const=const, isloop='aloop', mapper='records')
    step4 = Step(_step4, name='step_4', const=const, isloop='aloop')
    step5 = Step(_step5, name='step_5', const=const)
    # step6 = Step(_step6, name='step_6', const=const, isloop='aloop')
    # step7 = Step(_step7, name='step_7', const=const)
    # step8 = Step(_step8, name='step_8', const=const, isloop='aloop')
    # step9 = Step(_step9, name='step_9', const=const)
    # step10 = Step(_step10, name='step_10', const=const, isloop='aloop')

    pipe = step1 > step2
    pipe <= step3
    pipe <= step4
    pipe <= step5
    # pipe <= step6
    # pipe <= step7
    # pipe <= step8
    # pipe <= step9
    # pipe <= step10

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        if self.current_loop:
            OUT.print(f"Active {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")

    pipe.set_time_handler(time_handler)
    pipe.set_exec('28ac2cd8-5e3e-46dd-958b-1aa5ba670eeb', ['step_2', 'step_3'])

    asyncio.run(pipe.run_with_timer("Instagram Reels Duplicates"))

exit(0)







































from json import dump, load, dumps
from utils import deduplicate, add_error_handler
from cache import Cache
from sys import stdout
from pathlib import Path
from decimal import Decimal
from csv import DictWriter, DictReader

BASE = "Instagram"
IG_Accounts = "IG Accounts"
IG_Reels = "IG Reels"
Sounds = "Sounds"
VIEW = "Reels - Duplicate Test"
PAGES = 14
PARSED_PAGES = 2
PAGE_SIZE = 12

ONE_BUTCH_SIZE = 10


async def get_all_reels(user, callback=None):
    reels = await rapid.get_n_page(user, "reels", PAGES, user, callback)
    my_reels = reels["answer"]
    return [user, my_reels]


async def get_hash(record):
    hash_ = await L.load_hash(record['video_url'])
    record["video_hash"] = hash_
    return [record, hash_]


async def get_record(base, table, formula, record, id_):
    reel = await airtabel.search(
        base, table, maxRecords=1, filterByFormula=formula
    )
    record["reel_id"] = None
    if len(reel["records"]) > 0:
        reel_id = reel["records"][0]["id"]
        record["reel_id"] = reel_id
    else:
        reel_id = None
    return [id_, record]


async def get_reels_for(user_map):
    tasks = []
    for model, data in user_map.items():
        for user in data:
            tasks.append(get_all_reels(user['username']))
    reels = await asyncio.gather(*tasks)
    user_map_alt = {}
    for model, data in user_map.items():
        for user in data:
            user_map_alt[user['username']] = {
                "model": model,
                "reels": user["reels"]
            }
    records = []
    for user in reels:
        print(user[0], len(user[1]), user_map_alt[user[0]]['model'])
        for video in user[1]:
            try:
                caption = video.get("caption", {})
                if caption is None:
                    caption = {}
                clip_meta = video.get("clips_metadata", {})
                if clip_meta is None:
                    clip_meta = {}
                music_info = clip_meta.get("music_info", {})
                if music_info is None:
                    music_info = {}
                music_asset_info = music_info.get("music_asset_info", {})
                if music_asset_info is None:
                    music_asset_info = {}
                is_licensed = clip_meta.get("audio_type", "")
                original_sound_info = clip_meta.get("original_sound_info", {})
                if original_sound_info is None:
                    original_sound_info = {}
                music_consumption_info = music_info.get("music_consumption_info", {})
                if music_consumption_info is None:
                    music_consumption_info = {}
                records.append({
                    "username": user[0],
                    "model": user_map_alt[user[0]]['model'],
                    "video_id": video["id"],
                    "video_duration": video["video_duration"],
                    "video_url": video["video_url"],
                    "code": video["code"],
                    "caption_id": caption.get("id", ""),
                    "caption_text": caption.get("text", ""),
                    "music_type": is_licensed,
                    "audio_canonical_id": clip_meta.get("audio_canonical_id", ""),
                    "taken_at": video["taken_at"],
                    # licensed add markers
                    "audio_asset_start_time_in_ms": music_consumption_info.get("audio_asset_start_time_in_ms", 0),
                    "overlap_duration_in_ms": music_consumption_info.get("overlap_duration_in_ms", 0),
                    "audio_id": music_asset_info.get("audio_id", ""),
                    "title": music_asset_info.get("title", ""),
                    # origin add markers
                    "video_hash": "",
                    "originality": ""
                })
            except:
                logger.error(f"Video {video}, {format_exc()}")
    tasks = []
    for record in records:
        tasks.append(get_hash(record))
    await asyncio.gather(*tasks)

    records.sort(key=lambda obj: obj["taken_at"])

    _sources = {}
    data_for_update = {}

    def check_decimal(sources, value):
        return (
            value + Decimal("0.001") in sources or \
            value - Decimal("0.001") in sources or \
            value + Decimal("0.002") in sources or \
            value - Decimal("0.002") in sources or \
            value in sources
        )

    def set_potential_decimalal(sources, key, value):
        sources[key] = value
        sources[key + Decimal("0.001")] = value
        sources[key - Decimal("0.001")] = value
        sources[key + Decimal("0.002")] = value
        sources[key - Decimal("0.002")] = value

    def get_decimal(sources, key):
        _value_list = [
            key,
            key + Decimal("0.001"),
            key - Decimal("0.001"),
            key + Decimal("0.002"),
            key - Decimal("0.002")
        ]
        for value in _value_list:
            if value in sources:
                return sources[value]

    for record in records:
        if record['model'] not in _sources:
            _sources[record['model']] = {}
        sources = _sources[record['model']]
        try:
            record["originality"] = False
            hash_ = record["video_hash"]
            decimal_duration = Decimal(record["video_duration"])
            if record["music_type"] == "licensed_music":
                audio_id_and_duration = ("1", record["audio_id"], record["video_duration"])
                potential_sources_id = ("2", record["audio_id"], round(float(record["video_duration"]), 1))
            else:
                audio_id_and_duration = ("1", record["original_audio_id"], record["video_duration"])
                potential_sources_id = ("2", record["original_audio_id"], round(float(record["video_duration"]), 1))


            by_hash = hash_ in list(sources.keys())
            by_sound_and_duration = audio_id_and_duration in list(sources.keys())
            by_potential_sources = potential_sources_id in list(sources.keys())
            by_decimal = check_decimal(list(sources.keys()), decimal_duration)

            have_source = False
            is_potential = False
            if by_hash:
                source_ref = sources[hash_]
                have_source = True
            elif by_sound_and_duration:
                source_ref = sources[audio_id_and_duration]
                have_source = True
            elif by_potential_sources:
                source_ref = sources[potential_sources_id]
                have_source = True
                is_potential = True
            elif by_decimal:
                source_ref = get_decimal(sources, decimal_duration)
                have_source = True
                is_potential = True

            if not have_source:
                record["originality"] = True
                sources[hash_] = record
                sources[audio_id_and_duration] = record
                sources[potential_sources_id] = record
                set_potential_decimalal(sources, decimal_duration, record)
                data_for_update[record["video_id"]] = {
                    "duration": record["video_duration"],
                    "originality": True,
                    "is_potential": False,
                    "video_duration": record["video_duration"],
                    "video_duration_round": round(float(record["video_duration"]), 1),
                    "hash": record["video_hash"]
                }
            else:
                data_for_update[record["video_id"]] = {
                    "duration": record["video_duration"],
                    "originality": False,
                    "is_potential": is_potential,
                    "source": source_ref["video_id"],
                    "video_duration": record["video_duration"],
                    "video_duration_round": round(float(record["video_duration"]), 1),
                    "hash": record["video_hash"]
                }
                sources[hash_] = source_ref
                sources[audio_id_and_duration] = source_ref
                sources[potential_sources_id] = source_ref
                set_potential_decimalal(sources, decimal_duration, source_ref)

        except:
            logger.error(f"Video {video}, {format_exc()}")


    tasks = []
    for id_, data in data_for_update.items():
        if data["originality"]:
            search_reel = f'"{id_}"' + "={Reel ID}"
            tasks.append(get_record(BASE, IG_Reels, search_reel, data, id_))
    answer = await asyncio.gather(*tasks)
    for item in answer:
        data_for_update[item[0]] = item[1]
    tasks = []
    for id_ in data_for_update.keys():
        data = data_for_update[id_]
        if not data["originality"] and data["is_potential"]:
            if data_for_update[data["source"]].get("reel_id", None):
                search_reel = f'"{id_}"' + "={Reel ID}"
                tasks.append(get_record(BASE, IG_Reels, search_reel, data, id_))
    answer = await asyncio.gather(*tasks)
    for item in answer:
        data_for_update[item[0]] = item[1]
    tasks = []
    for id_ in data_for_update.keys():
        data = data_for_update[id_]
        if not data["originality"] and not data["is_potential"]:
            if data_for_update[data["source"]].get("reel_id", False):
                search_reel = f'"{id_}"' + "={Reel ID}"
                tasks.append(get_record(BASE, IG_Reels, search_reel, data, id_))
    answer = await asyncio.gather(*tasks)
    for item in answer:
        data_for_update[item[0]] = item[1]
    batches = []
    none_batches = []

    for id_ in data_for_update.keys():
        data = data_for_update[id_]
        if data.get("reel_id", None):
            originality = "Original Content" if data["originality"] else "Duplicate Content"
            if data["is_potential"]:
                originality = "Potential Duplicate"
            update_data = {
                "id": data["reel_id"],
                "fields": {
                    "Link to Original Content": [],
                    "Duplicate / Original": originality,
                    "Video hash": data["hash"],
                    "Reel Duration": data["video_duration"],
                    "Video rounded duration": data["video_duration_round"]
                }
            }
            link_to_origin = data_for_update.get(data.get("source", None), {}).get("reel_id", None)
            if link_to_origin and not data["originality"]:
                update_data["fields"]["Link to Original Content"] = [link_to_origin]
            batches.append(update_data)
    batches.sort(key=lambda x: x["fields"]["Duplicate / Original"] == "Original Content")

    return batches


def make_users_map(users):
    user_map = {}
    for user in users:
        if "Model" in user["fields"]:
            if len(user["fields"]["Model"]) == 1:
                model = user["fields"]["Model"][0]
                username = user["fields"]["Username"]
                if model not in user_map:
                    user_map[model] = []
                user_map[model].append({
                    "username": username,
                    "reels": user["fields"].get("IG Reels", [])
                })
    return user_map


async def upsert(i, records):
    try:
        answer = await airtabel.upsert(BASE, IG_Reels, None, records)
    except Exception:
        error_logger.error(f"ERROR IG Reels {EXEC_ID}")
        error_logger.error(format_exc())
        logger.error(format_exc())
        error_logger.error(f"{records}")
    print(f"End {i}")


async def get_records():
    view = airtabel.local_cache[f"shema_{IG_Accounts}_views"][VIEW]
    execution_logger.info("Get Users")
    users = await airtabel.search_until(
        BASE, IG_Accounts, None, view,
        ["Username", "Model", "IG Reels"]
    )
    stdout.write(f"List len: {len(users['records'])}\n")
    user_map = make_users_map(users["records"])
    batches = await get_reels_for(user_map)
    execution_logger.info("Split them on chunks")

    tasks = []
    for i, chunk in enumerate(chunks(batches, ONE_BUTCH_SIZE)):
        tasks.append(upsert(i, chunk))
    await asyncio.gather(*tasks)

async def main():
    global _need_ping

    airtabel.load_cache("airtable_db.json")
    execution_logger.info("Airtable Cache Loaded")
    await airtabel.init(BASE)
    execution_logger.info("Airtable Init Finished")
    airtabel.dump_cache("airtable_db.json")
    execution_logger.info("Airtable Cache Dumped")

    await get_records()
    _need_ping = False
