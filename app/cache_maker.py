import asyncio
import libs.init

from libs.api import AirtableApi, _request_counter
from libs.settings import AIRTABLE_TOKEN
from libs.utils import chunks
from datetime import datetime
from libs.makepipeline import Step, Const, OUT
from cache.models import Reel, Sound
from asgiref.sync import sync_to_async

const = Const(
    BASE='Instagram',
    REELS='IG Reels',
    REELS_VIEW='Reels For Cache',
    SOUNDS_VIEW='All Sounds',
    SOUNDS='Sounds',
    REELS_FIELDS=["Name", "Reel ID"],
    SOUNDS_FIELDS=["Sound Title", "Sound ID"]
)

airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN
)

if __name__ == '__main__':

    async def _step1(data: None, context, const, pipe, iteration, step_number) -> None:
        airtabel.load_cache("airtable_db.json")
        await airtabel.init(const.BASE)
        airtabel.dump_cache("airtable_db.json")

    async def _step2(data: None, context, const, pipe, iteration, step_number) -> dict:
        reels = await airtabel.search_until(
            const.BASE, const.REELS, None, const.REELS_VIEW,
            const.REELS_FIELDS
        )
        sounds = await airtabel.search_until(
            const.BASE, const.SOUNDS, None, const.SOUNDS_VIEW,
            const.SOUNDS_FIELDS
        )
        return [reels, sounds]

    async def _step3(data: dict, context, const, pipe, iteration, step_number) -> list:
        reels, sounds = data
        for_create_reels = []
        for_create_sounds = []
        for reel in reels['records']:
            id_ = reel['fields'].get('Reel ID', '0')
            if id_ != '0':
                for_create_reels.append({
                    'airtabel_id': reel['id'],
                    'rapid_id': id_
                })
        for sound in sounds['records']:
            id_ = sound['fields'].get('Sound ID', '0')
            if id_ != '0':
                for_create_sounds.append({
                    'airtabel_id': sound['id'],
                    'rapid_id': id_
                })
        return [for_create_reels, for_create_sounds]

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> list:
        reels, sounds = data
        reels_tasks = []
        sound_tasks = []
        reel_check = []
        for reels in list(chunks(reels, 20)):
            batch = []
            for reel in reels:
                if reel['rapid_id'] not in reel_check:
                    reel_check.append(reel['rapid_id'])
                    batch.append(Reel(**reel))
            reels_tasks.append(batch)
        del reel_check

        sound_check = []
        for sounds in list(chunks(sounds, 20)):
            batch = []
            for sound in sounds:
                if sound['rapid_id'] not in sound_check:
                    sound_check.append(sound['rapid_id'])
                    batch.append(Reel(**sound))
            sound_tasks.append(batch)
        del sound_check

        async_reel_create = sync_to_async(
            Reel.objects.bulk_create, thread_sensitive=True
        )
        async_sound_create = sync_to_async(
            Sound.objects.bulk_create, thread_sensitive=True
        )
        tasks = []
        for reels in reels_tasks:
            tasks.append(async_reel_create(reels))
        for sounds in sound_tasks:
            tasks.append(async_sound_create(sounds))
        await asyncio.gather(*tasks)

    step1 = Step(_step1, name='step_1', const=const)
    step2 = Step(_step2, name='step_2', const=const)
    step3 = Step(_step3, name='step_3', const=const)
    step4 = Step(_step4, name='step_4', const=const)

    pipe = step1 > step2
    pipe <= step3
    pipe <= step4

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        if self.current_loop:
            OUT.print(f"Active {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")

    pipe.set_time_handler(time_handler)

    asyncio.run(pipe.run_with_timer("Make Cache"))
