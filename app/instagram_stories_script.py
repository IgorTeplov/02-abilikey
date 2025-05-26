import asyncio

from libs.api import AirtableApi, RapidApi, Loader, _request_counter
from libs.settings import AIRTABLE_TOKEN, XRapidAPIHost, XRapidAPIKey
from libs.utils import chunks
from datetime import datetime
from libs.logs import execution_logger, error_logger
from libs.makepipeline import Step, Const, OUT

const = Const(
    BASE='Instagram',
    ACCOUNTS='IG Accounts',
    MODEL='IG Stories',
    URL_MODEL='stories',
    SOUNDS='Sounds',
    ACCOUNTS_VIEW="Stories - To be scraped",
    PAGES=2,
    USER_COUNT=None,
    FIELDS=["Username", "Follower", "Created", "Account ID", "IG Reels"]
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

    async def get_hash(post):
        if post["is_video"]:
            root = post["video_versions"]
        else:
            root = post["image_versions"]["items"]

        media = {}
        for item in root:
            media[item["url"].split("?")[0].split("/")[-1]] = item["url"]
        media = [{"url": v, "filename": k} for k, v in media.items()]

        hash_ = []
        for i in media:
            hash_.append(await L.load_hash(i['url']))
        hash_ = ";".join(hash_)
        return hash_, media

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
        answer = await rapid.get_n_page(username, const.URL_MODEL, const.PAGES)
        models = answer.get('answer', [])
        models.sort(key=lambda item: -item['taken_at'])
        return {'models': models, 'user': data, 'status': answer.get('status')}

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> list:
        stories = data['models']
        user = data['user']
        accaunt_created = datetime.strptime(
            user["fields"]["Created"], "%Y-%m-%d"
        )
        new_stories = []
        for i, story in enumerate(stories):
            if i + 1 < len(stories):
                story['_diff'] = round(
                    (story['taken_at'] - stories[i + 1]['taken_at']) / 3600, 2
                )
            post_taken_at = datetime.fromtimestamp(story["taken_at"])
            if post_taken_at >= accaunt_created:
                story['_user'] = {
                    'id': user['id']
                }
                new_stories.append(story)
        execution_logger.info(
            f"{len(new_stories)}/{len(stories)} {user['fields']['Username']}"
        )
        return new_stories

    async def _step5(data: list, context, const, pipe, iteration, step_number) -> list:
        sounds = {}
        for post in data:
            if post.get("music_metadata", None) is not None: 
                if post.get("music_metadata", {}).get("audio_type", "") == "licensed_music":
                    _data = post.get("music_metadata", {})
                    if _data is not None:
                        _data = _data.get("music_info", {})
                        if _data is not None:
                            _data = _data.get("music_asset_info", None)
                    if _data is not None:
                        sounds[post["id"]] = {
                            "fields": {
                                'Sound ID': str(_data["audio_id"]),
                                'Sound Title': str(_data["title"]),
                                'Sound Type': "Licensed Music"
                            }
                        }
                else:
                    _data = post.get("music_metadata", {})
                    if _data is not None:
                        _data = _data.get("original_sound_info", None)
                    if _data is not None:
                        sounds[post["id"]] = {
                            "fields": {
                                'Sound ID': str(_data["audio_id"]),
                                'Sound Title': str(_data["original_audio_title"]),
                                'Sound Type': "Original Sound"
                            }
                        }

        ai_sounds = []
        for butch in list(chunks(list(sounds.values()), 10)):
            if len(butch) > 0:
                lsounds = await airtabel.upsert(
                    const.BASE, const.SOUNDS, ["Sound ID"], butch
                )
                if "error" in lsounds:
                    error_logger.error(f'Bad Sounds - {lsounds["error"]} in this batch {butch}')
                if "records" in lsounds:
                    ai_sounds += lsounds["records"]

        ai_sound_map = {}
        for item in ai_sounds:
            ai_sound_map[item['fields']['Sound ID']] = item['id']
        execution_logger.info(f"Posts with sounds: {list(sounds.keys())}")
        execution_logger.info(f"Created Sounds: {ai_sound_map}")
        for post in data:
            post['_sound'] = {
                "Sound ID": None,
                "Sound": []
            }
            if post['id'] in sounds:
                SoundID = sounds[post['id']]['fields']['Sound ID']
                if SoundID in ai_sound_map:
                    post['_sound'] = {
                        "Sound ID": SoundID,
                        "Sound": [ai_sound_map[SoundID]]
                    }
            execution_logger.info(f"{post['id']} - {post['_sound']}")
        return data

    async def _step6(data: list, context, const, pipe, iteration, step_number) -> list:
        _data = []
        for items in data:
            _data += items
        execution_logger.info(f"Total: {len(_data)}")
        return _data

    async def _step7(data: dict, context, const, pipe, iteration, step_number) -> dict:
        post = data

        hash_, media = await get_hash(post)
        hash_find = f'"{hash_}"' + "={Hash}"
        is_origin = '"Original Content"={Duplicate / Original}'
        hash_find = f'AND({hash_find}, {is_origin})'
        origin_post = await airtabel.search_by_formula(
            const.BASE, const.MODEL, hash_find
        )
        origin = "Original Content"
        if origin_post:
            origin = "Duplicate Content"

        update = {
            "Account": [post['_user']["id"]],
            "Story ID": str(post["id"]),
            "Media Format": "Video" if post["is_video"] else "Image",
            "File": media,
            "Hash": hash_,
            "Duplicate / Original": origin,
            "Posting Time": post["taken_at"] * 1000,
            "Posting Date": datetime.fromtimestamp(
                post["taken_at"]
            ).strftime("%Y-%m-%d"),
        }
        if origin_post:
            update["Link to Original Content"] = [origin_post]

        if '_sound' in post and post['_sound']['Sound ID']:
            update["Sound"] = post['_sound']['Sound']
            update["Sound ID"] = post['_sound']['Sound ID']

        return {"fields": update}

    async def _step8(data: list, context, const, pipe, iteration, step_number) -> list:
        return list(chunks(data, 10))

    async def _step9(data: list, context, const, pipe, iteration, step_number) -> list:
        answer = await airtabel.upsert(
            const.BASE, const.MODEL, ["Story ID"], data
        )
        if 'error' in answer:
            error_logger.error(f"Bad story upsert: {data}, error: {answer}")
        return answer

    async def _step10(data: list, context, const, pipe, iteration, step_number) -> list:
        update = 0
        create = 0
        for part in data:
            update += len(part['updatedRecords'])
            create += len(part['createdRecords'])
        execution_logger.info(f"Created: {create} Updated: {update}")

    async def _step11(data: dict, context, const, pipe, iteration, step_number) -> list:
        data = []
        for item in context[3]['out']:
            data.append({
                'id': item['user']['id'],
                'fields': {
                    "Status": item['status'],
                    "Stories - Last Scraped": datetime.today().strftime("%Y-%m-%d")
                }
            })
        return list(chunks(data, 10))

    async def _step12(data: dict, context, const, pipe, iteration, step_number) -> dict:
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
    step5 = Step(_step5, name='step_5', const=const, isloop='aloop')
    step6 = Step(_step6, name='step_6', const=const)
    step7 = Step(_step7, name='step_7', const=const, isloop='aloop')
    step8 = Step(_step8, name='step_8', const=const)
    step9 = Step(_step9, name='step_9', const=const, isloop='aloop')
    step10 = Step(_step10, name='step_10', const=const)
    step11 = Step(_step11, name='step_11', const=const)
    step12 = Step(_step12, name='step_12', const=const, isloop='aloop')

    pipe = step1 > step2
    pipe <= step3
    pipe <= step4
    pipe <= step5
    pipe <= step6
    pipe <= step7
    pipe <= step8
    pipe <= step9
    pipe <= step10
    pipe <= step11

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        if self.current_loop:
            OUT.print(f"Active {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")

    pipe.set_time_handler(time_handler)

    asyncio.run(pipe.run_with_timer("Instagram Stories"))
