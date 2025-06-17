import asyncio
import libs.init

from libs.api import AirtableApi, RapidApi, Loader, _request_counter
from libs.settings import AIRTABLE_TOKEN, XRapidAPIHost, XRapidAPIKey
from libs.utils import chunks
from datetime import datetime
from libs.logs import execution_logger, error_logger
from libs.makepipeline import Step, Const, OUT
from cache.models import Sound


const = Const(
    BASE='Instagram',
    ACCOUNTS='IG Accounts',
    POSTS='IG Posts',
    URL_MODEL='posts',
    SOUNDS='Sounds',
    ACCOUNTS_VIEW="Posts - To be scraped",
    PAGES=14,
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
        carousel_media = post.get("carousel_media", [])
        if carousel_media:
            hash_ = []
            for i in carousel_media:
                if i["is_video"]:
                    hash_.append(await L.load_hash(i["video_url"]))
                else:
                    hash_.append(await L.load_hash(i["image_versions"]["items"][0]["url"]))
            return ";".join(hash_)
        else:
            return await L.load_hash(post["image_versions"]["items"][0]["url"])

    async def get_sound(model, const):
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
        if sound:
            try:
                cache_obj = await Sound.objects.aget(rapid_id=sound['Sound ID'])
            except:
                ai_sound = await airtabel.create(const.BASE, const.SOUNDS, sound)
                sound_id = ai_sound["id"]
                cache_obj = Sound(
                    rapid_id=sound['Sound ID'],
                    airtabel_id=sound_id
                )
                await cache_obj.asave()
            error_logger.info(f'Sound found - {model}')
            return cache_obj

        else:
            return None

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
        models.sort(key=lambda item: -item.get('taken_at', 0))
        return {'models': models, 'user': data, 'status': answer.get('status')}

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> list:
        posts = data['models']
        user = data['user']
        accaunt_created = datetime.strptime(
            user["fields"]["Created"], "%Y-%m-%d"
        )
        new_posts = []
        for i, post in enumerate(posts):
            if i + 1 < len(posts):
                post['_diff'] = round(
                    (post['taken_at'] - posts[i + 1]['taken_at']) / 3600, 2
                )
            is_pinned = post.get("is_pinned", True)
            is_video = post.get("is_video", True)
            if not is_pinned and not is_video:
                post_taken_at = datetime.fromtimestamp(post["taken_at"])
                if post_taken_at >= accaunt_created:
                    post['_user'] = {
                        'id': user['id']
                    }
                    new_posts.append(post)
        execution_logger.info(
            f"{len(new_posts)}/{len(posts)} {user['fields']['Username']}"
        )
        return new_posts

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

        hash_ = await get_hash(post)
        hash_find = f'"{hash_}"' + "={Hash}"
        is_origin = '"Original Content"={Duplicate / Original}'
        hash_find = f'AND({hash_find}, {is_origin})'
        origin_post = await airtabel.search_by_formula(
            const.BASE, const.POSTS, hash_find
        )
        origin = "Original Content"
        if origin_post:
            origin = "Duplicate Content"

        update = {
            "Account": [post['_user']["id"]],
            "Post URL": f"https://www.instagram.com/p/{post['code']}",
            "Post ID": str(post["id"]),
            "Likes": post["like_count"],
            "Comments": post["comment_count"],
            "Hash": hash_,
            "Duplicate / Original": origin,
            "Image Count": post.get("carousel_media_count", 1),
            "Posting Time": post["taken_at"] * 1000,
            "Posting Date": datetime.fromtimestamp(
                post["taken_at"]
            ).strftime("%Y-%m-%d"),
        }
        if origin_post:
            update["Link to Original Content"] = [origin_post]
        if "caption" in post:
            if isinstance(post["caption"], dict) and "text" in post["caption"]:
                update["Caption"] = post["caption"]["text"]

        sound_data = await get_sound(data, const)
        if sound_data:
            update["Sounds"] = [sound_data.airtabel_id]
            update["Sound"] = sound_data.rapid_id

        # if '_sound' in post and post['_sound']['Sound ID']:
        #     update["Sound"] = post['_sound']['Sound']
        #     update["Sound ID"] = post['_sound']['Sound ID']

        if '_diff' in post:
            update["Difference (Hours) to previous Post"] = post['_diff']
        return {"fields": update}

    async def _step8(data: list, context, const, pipe, iteration, step_number) -> list:
        return list(chunks(data, 10))

    async def _step9(data: list, context, const, pipe, iteration, step_number) -> list:
        answer = await airtabel.upsert(
            const.BASE, const.POSTS, ["Post ID"], data
        )
        if 'error' in answer:
            error_logger.error(f"Bad post upsert: {data}, error: {answer}")
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
                    "Posts - Last Scraped": datetime.today().strftime("%Y-%m-%d")
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
    # step5 = Step(_step5, name='step_5', const=const, isloop='aloop')
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
    # pipe <= step5
    pipe <= step6
    pipe <= step7
    pipe <= step8
    pipe <= step9
    pipe <= step10
    pipe <= step11
    pipe <= step12

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        if self.current_loop:
            OUT.print(f"Active {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")
        self.write_status(self.get_status())

    pipe.set_time_handler(time_handler)

    asyncio.run(pipe.run_with_timer("Instagram Posts"))
