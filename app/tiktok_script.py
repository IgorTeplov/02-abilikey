import asyncio
from libs.api import RapidTikTokApi, AirtableApi, _request_counter
from libs.settings import AIRTABLE_TOKEN_TikTok, XRapidAPIHostTikTok, XRapidAPIKey
from libs.makepipeline import Step, Const, OUT
from datetime import datetime
from libs.utils import chunks
from libs.logs import error_logger, execution_logger
import psutil
from os import getpid
import re

from sys import argv

if len(argv) == 1:
    exit()

airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN_TikTok
)
rapid = RapidTikTokApi(
    "https://scraptik.p.rapidapi.com/",
    {"X-RapidAPI-Key": XRapidAPIKey, "X-RapidAPI-Host": XRapidAPIHostTikTok}
)

# Backup Scraping List
# To be scraped today - Posting Farm
# To be scraped today - No Posting Farm

# All tracked Accounts
# To be scraped today - Posting Farm TEST
# To be scraped today - No Posting Farm TEST

const = Const(
    BASE='Account Management',
    ACCOUNTS='TikTok Accounts',
    ACCOUNTS_VIEW=' '.join(argv[1:]),
    VIDEOS='TikTok Videos',
    IG_ACCOUNTS='Instagram Accounts',
    USER_COUNT=None,
    POST_COUNT=80,
    MARK=True
)

FIELDS = [
    "Account ID", "Created", "Model Record ID", "Username",
    "Average Views to calculate Viral Videos", "Follower",
    "Total Account Views (Started: 01.08.2024)"
]

pid = getpid()
proc = psutil.Process(pid)

if __name__ == '__main__':

    async def _step1(data: None, context, const, pipe, iteration, step_number) -> None:
        # Init Airtable
        airtabel.load_cache("airtable_tiktok_db.json")
        await airtabel.init(const.BASE)
        airtabel.dump_cache("airtable_tiktok_db.json")

    async def _step2(data: None, context, const, pipe, iteration, step_number) -> list:
        # Get Accounts
        if const.USER_COUNT is not None:
            data = await airtabel.search(
                const.BASE, const.ACCOUNTS, const.USER_COUNT,
                const.ACCOUNTS_VIEW, FIELDS
            )
        data = await airtabel.search_until(
            const.BASE, const.ACCOUNTS, None,
            const.ACCOUNTS_VIEW, FIELDS
        )
        execution_logger.info(f"GET {len(data['records'])} models")
        return data

    async def _step3(data: dict, context, const, pipe, iteration, step_number) -> dict | None:
        # Update Account
        username = data['fields']['Username']
        _data = (await rapid.get_user(username)).get('user', None)
        if _data:
            update = {
                'Last Scraped': datetime.today().strftime("%Y-%m-%d"),
                "Status": "Active",
                "Follower": _data.get('follower_count', 0),
                "Bio": _data.get('signature', ''),
                "Display Name": _data.get('nickname', ''),
                "Following": _data.get('following_count', 0),
                "Video Count": _data.get('aweme_count', 0),
                "Like Count": _data.get('total_favorited', 0),
                "Account ID": _data.get('uid'),
                "secUID": _data.get('sec_uid'),
                "Follower (T-1)": data['fields'].get('Follower', 0),
                "Total Account Views (T-1)": data['fields'].get(
                    'Total Account Views (Started: 01.08.2024)', '0'
                )
            }
            if _data.get('avatar_medium', None):
                if _data.get('avatar_medium').get('url_list', None):
                    update['Profile Picture'] = [{
                        "url": _data.get('avatar_medium').get('url_list')[0],
                        "filename": "avatar"
                    }]
            if const.MARK:
                answer = await airtabel.update(
                    const.BASE, const.ACCOUNTS, data['id'], update
                )
                if "error" in answer:
                    error_logger.error(
                        f"Bad user update: {data}, error: {answer}"
                    )
            data['fields']['Account ID'] = _data.get('uid')
            ig_acc = _data.get('ins_id', None)
            if not ig_acc:
                match = re.match(
                    r"instagram\.com/(?P<ig_id>[^/?#]+)",
                    _data.get('bio_url', '')
                )
                if match is not None:
                    ig_acc = match.group('ig_id')

            if ig_acc:
                check = await airtabel.search_by_formula(
                    const.BASE, const.IG_ACCOUNTS,
                    f'LOWER(TRIM("{ig_acc}"))' + "=LOWER(TRIM({Username}))"
                )
                if check:
                    answer = await airtabel.update(
                        const.BASE, const.ACCOUNTS, data['id'],
                        {'Automatic - Linked Instagram Accounts': [check]}
                    )
                    if "error" in answer:
                        error_logger.error(f"Bad user update: {data}, error: {answer}")

            return data
        else:
            if const.MARK:
                answer = await airtabel.update(
                    const.BASE, const.ACCOUNTS, data['id'],
                    {
                        'Last Scraped': datetime.today().strftime("%Y-%m-%d"),
                        'Status': "Potential Ban"
                    }
                )
                if "error" in answer:
                    error_logger.error(f"Bad user update: {data}, error: {answer}")

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> list | None:
        fields = data.get('fields', {})
        if fields:
            username = fields.get('Account ID', None)
            if username:
                _data = (await rapid.get_n_page(
                    username, count=const.POST_COUNT
                )).get('aweme_list', None)

                if isinstance(_data, list):
                    execution_logger.info(f"GET {len(_data)} records for {username} {fields['Username']}")
                else:
                    execution_logger.info(f"Error for {username}")

                if _data:
                    _data = [item for item in _data if item.get('is_top', None) not in ['1', None]]
                    _data.sort(key=lambda item: -item['create_time'])
                    for i, item in enumerate(_data):
                        item["__meta"] = data
                        if i != len(_data) - 1:
                            item['difftime'] = (item["create_time"] - _data[i+1]["create_time"]) / 3600
                    return _data
                else:
                    if const.MARK:
                        await airtabel.update(
                            const.BASE, const.ACCOUNTS, data['id'], {
                                'Last Scraped': datetime.today().strftime("%Y-%m-%d"),
                                'Status': "Potential Ban"
                            }
                        )

    async def _step5(data: list, context, const, pipe, iteration, step_number) -> list:
        _data = []
        for items in data:
            _data += items
        execution_logger.info(f"Total: {len(_data)}")
        return _data

    async def _step6(data: dict, context, const, pipe, iteration, step_number) -> dict | None:
        is_top = data.get('is_top', None)
        create_time = data.get('create_time', None)
        aweme_id = data.get('aweme_id', None)
        statistics = data.get('statistics', None)
        __meta = data.get('__meta', {})
        sound = None
        if is_top is not None and create_time is not None and aweme_id is not None and statistics is not None:
            if is_top != '1' and datetime.fromtimestamp(create_time) > datetime.fromisoformat(__meta['fields']['Created'][:-1]):
                av = __meta['fields'].get(
                    'Average Views to calculate Viral Videos', None
                )
                answer = {
                    "Link to Model": __meta['fields']['Model Record ID'],
                    "Views": statistics["play_count"],
                    "Likes": statistics["digg_count"],
                    "Comments": statistics["comment_count"],
                    "Shares": statistics["share_count"],
                    "Posting Time": create_time * 1000,
                    "Difference (Hours) to previous Post": abs(data.get('difftime', 0)),
                    "Account": [__meta['id']],
                    "Posting Date": datetime.fromtimestamp(create_time).strftime("%Y-%m-%d"),
                    "Caption": data.get('desc', ''),
                    "VideoID": aweme_id,
                }
                if av is not None:
                    answer["Average Views of non-viral Videos - 1 week from posting"] = float(av)
                added_sound_music_info = data.get('added_sound_music_info', None)
                if added_sound_music_info:
                    answer['Sound Title'] = added_sound_music_info.get(
                        'title', ''
                    )
                    sound = added_sound_music_info.get('id', '')
                if sound:
                    answer['Sound ID'] = str(added_sound_music_info.get('id', ''))
                return {"fields": answer}

    async def _step7(data: list, context, const, pipe, iteration, step_number) -> list:
        execution_logger.info(f"After filter: {len(data)}")
        answer = list(chunks(data, 10))
        return answer

    async def _step8(data: list, context, const, pipe, iteration, step_number) -> dict:
        answer = await airtabel.upsert(
            const.BASE, const.VIDEOS, ["VideoID"], data
        )
        if 'error' in answer:
            error_logger.error(f"Bad video upsert: {data}, error: {answer}")
        return answer

    async def _step9(data: list, context, const, pipe, iteration, step_number) -> None:
        total_updated = 0
        total_created = 0
        for batch in data:
            total_updated += len(batch.get('updatedRecords', []))
            total_created += len(batch.get('createdRecords', []))
        execution_logger.info(
            "After check: total_updated: "
            f"{total_updated}, total_created: {total_created}"
        )

    step1 = Step(_step1, name='step_1', const=const)
    step2 = Step(_step2, name='step_2', const=const)
    step3 = Step(_step3, name='step_3', const=const, isloop='aloop', mapper='records')
    step4 = Step(_step4, name='step_4', const=const, isloop='aloop')
    step5 = Step(_step5, name='step_5', const=const)
    step6 = Step(_step6, name='step_6', const=const, isloop='aloop')
    step7 = Step(_step7, name='step_7', const=const)
    step8 = Step(_step8, name='step_8', const=const, isloop='aloop')
    step9 = Step(_step9, name='step_9', const=const)

    pipe = step1 > step2
    pipe <= step3
    pipe <= step4
    pipe <= step5
    pipe <= step6
    pipe <= step7
    pipe <= step8
    pipe <= step9

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        if self.current_loop:
            OUT.print(f"Active {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")
        mem = round(proc.memory_info().rss / 1024 ** 2, 3)
        OUT.print(f"Memory Used: {pid} - {mem} MB")

    pipe.set_time_handler(time_handler)

    asyncio.run(pipe.run_with_timer(
        f"TikTok Scraping for {const.ACCOUNTS_VIEW}"
    ))
