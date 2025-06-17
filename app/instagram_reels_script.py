import asyncio
import libs.init
from asgiref.sync import sync_to_async

from libs.api import AirtableApi, RapidApi, _request_counter
from libs.settings import AIRTABLE_TOKEN, XRapidAPIHost, XRapidAPIKey
from libs.utils import chunks
from datetime import datetime
from libs.logs import execution_logger, error_logger
from libs.makepipeline import Step, Const, OUT
from cache.models import Sound, Reel
from traceback import format_exc

const = Const(
    BASE='Instagram',
    ACCOUNTS='IG Accounts',
    MODEL='IG Reels',
    SOUNDS='Sounds',
    URL_MODEL='reels',
    ACCOUNTS_VIEW="Reels - To be scraped TEST",
    PAGES=7,
    USER_COUNT=5,
    FIELDS=["Username", "Follower", "Created", "Account ID", "IG Reels"]
)

airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN
)
rapid = RapidApi(
    "https://instagram-scraper-20252.p.rapidapi.com/v1/",
    {"X-RapidAPI-Key": XRapidAPIKey, "X-RapidAPI-Host": XRapidAPIHost}
)

if __name__ == '__main__':

    get_sound_object = sync_to_async(Sound.objects.get, thread_sensitive=True)
    get_reel_object = sync_to_async(Reel.objects.get, thread_sensitive=True)

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
                cache_obj = await get_sound_object(rapid_id=sound['Sound ID'])
            except Exception:
                error_logger.error(f'{sound["Sound ID"]} ERROR: {format_exc()}')
                ai_sound = await airtabel.create(const.BASE, const.SOUNDS, sound)
                sound_id = ai_sound["id"]
                cache_obj = Sound(
                    rapid_id=sound['Sound ID'],
                    airtabel_id=sound_id
                )
                await cache_obj.asave()
            return cache_obj

        else:
            error_logger.error(f'Sound not found - {model}')
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
        models.sort(key=lambda item: -item['taken_at'])
        return {'models': models, 'user': data, 'status': answer.get('status')}

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> list:
        reels = data['models']
        user = data['user']
        accaunt_created = datetime.strptime(
            user["fields"]["Created"], "%Y-%m-%d"
        )
        new_reels = []
        for i, reel in enumerate(reels):
            if i + 1 < len(reels):
                reel['_diff'] = round(
                    (reel['taken_at'] - reels[i + 1]['taken_at']) / 3600, 2
                )
            is_pinned = reel.get("is_pinned", True)
            if not is_pinned:
                post_taken_at = datetime.fromtimestamp(reel["taken_at"])
                if post_taken_at >= accaunt_created:
                    reel['_user'] = {
                        'id': user['id']
                    }
                    new_reels.append(reel)
        execution_logger.info(
            f"{len(new_reels)}/{len(reels)} {user['fields']['Username']}"
        )
        return new_reels

    async def _step5(data: list, context, const, pipe, iteration, step_number) -> list:
        _data = []
        for items in data:
            _data += items
        execution_logger.info(f"Total: {len(_data)}")
        return _data

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

        # update cache
        if 'records' in answer:
            for record in answer['records']:
                airtabel_id = record['id']
                rapid_id = record['fields']['Reel ID']

                try:
                    await get_reel_object(airtabel_id=airtabel_id)
                except Exception:
                    reel = Reel(airtabel_id=airtabel_id, rapid_id=rapid_id)
                    await reel.asave()

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
    step6 = Step(_step6, name='step_6', const=const, isloop='aloop')
    step7 = Step(_step7, name='step_7', const=const)
    step8 = Step(_step8, name='step_8', const=const, isloop='aloop')
    step9 = Step(_step9, name='step_9', const=const)
    step10 = Step(_step10, name='step_10', const=const, isloop='aloop')

    pipe = step1 > step2
    pipe <= step3
    pipe <= step4
    pipe <= step5
    pipe <= step6
    pipe <= step7
    pipe <= step8
    pipe <= step9
    pipe <= step10

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        if self.current_loop:
            OUT.print(f"Active {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")
        self.write_status(self.get_status())

    pipe.set_time_handler(time_handler)

    asyncio.run(pipe.run_with_timer("Instagram Reels"))
