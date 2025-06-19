import asyncio
import libs.init
from libs.api import AirtableApi, _request_counter, Loader, RapidTwitterApi, LinkAnalizer, RapidApi
from libs.settings import AIRTABLE_TOKEN_Twitter, XRapidAPIHostTwitter, XRapidAPIKey, XRapidAPIHost
from libs.makepipeline import Step, Const, OUT
from libs.logs import error_logger, execution_logger
from datetime import datetime
from libs.utils import chunks
from cache.models import TwitterUser
from asgiref.sync import sync_to_async
import re


airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN_Twitter
)
rapid_v1 = RapidTwitterApi(
    "https://twitter135.p.rapidapi.com/v1.1/",
    {"X-RapidAPI-Key": XRapidAPIKey, "X-RapidAPI-Host": XRapidAPIHostTwitter}
)
rapid_v2 = RapidTwitterApi(
    "https://twitter135.p.rapidapi.com/v2/",
    {"X-RapidAPI-Key": XRapidAPIKey, "X-RapidAPI-Host": XRapidAPIHostTwitter}
)
rapid_inst = RapidApi(
    "https://instagram-scraper-20252.p.rapidapi.com/v1/",
    {"X-RapidAPI-Key": XRapidAPIKey, "X-RapidAPI-Host": XRapidAPIHost}
)

const = Const(
    BASE='Twitter E-Mail Scraping Automation',
    FOLLOWERS='Scraped Followers',
    ACCOUNTS='Twitter Profiles',
    VIEW_BACKUP='Empty OF URL',
    VIEW='Followers not scraped',
    FIELDS=['username', 'User ID'],
    FIELDS_BACKUP=[
        "username", "Profile URL", "Display Name", "User ID", "Follower",
        "Profile Picture", "Posts", "Bio", "Link in Bio", "E-Mail", "Gender",
        "OnlyFans URL 1", "OnlyFans URL 2", "Gender Determination Tries",
        "Preselection", "Gender determined", "Last Tweet", "OF Link not Found",
        "OnlyFans Username 1", "OnlyFans Username 2", "Twitter DMs open",
        "Record ID (Twitter Database)", "Not suitable", "Scraped from",
        "Created",
    ],
    RECORD_COUNT=50,
    PROD=False,
    DB_FOR_VALIDATION=[
        {"BASE": "Twitter E-Mail Scraping Automation", "TABLE": "Scraped Followers"},
        {"BASE": "Twitter E-Mail Scraping Automation", "TABLE": "Twitter Profiles"}
    ],
    DB_local=True,
    Ignore_Duplicates=False
)

L = Loader()
LA = LinkAnalizer('./linkanalizer.json', L, rapid_inst)

if __name__ == '__main__':

    aget = sync_to_async(TwitterUser.objects.get, thread_sensitive=True)
    acreate = sync_to_async(TwitterUser.objects.create, thread_sensitive=True)

    async def check_user(airtabel_id=None, rapid_id=None):
        if airtabel_id is not None:
            try:
                await aget(airtabel_id=airtabel_id)
                return True
            except Exception:
                pass
        if rapid_id is not None:
            try:
                await aget(rapid_id=rapid_id)
                return True
            except Exception:
                pass
        return False

    async def check_tuser(id_, const):
        if const.Ignore_Duplicates:
            return True
        if not const.DB_local:
            for db in const.DB_FOR_VALIDATION:
                answer = await airtabel.search_by_formula(db['BASE'], db['TABLE'], '{User ID}='+f"'{id_}'")
                if answer:
                    return False
        else:
            if await check_user(rapid_id=id_):
                message = f'User {id_} exexist in local db!'
                execution_logger.info(message)
                OUT.print(message)
                return False
        return True


    async def _step1(data: None, context, const, pipe, iteration, step_number) -> None:
        # Init Airtable
        # airtabel.load_cache("airtable_twitter_db.json")
        await airtabel.init(const.BASE)
        airtabel.dump_cache("airtable_twitter_db.json")

    async def _step2(data: None, context, const, pipe, iteration, step_number) -> dict:
        # Get Accounts
        if const.RECORD_COUNT is not None:
            data = await airtabel.search(
                const.BASE, const.ACCOUNTS, const.RECORD_COUNT,
                const.VIEW, const.FIELDS
            )
        else:
            data = await airtabel.search_until(
                const.BASE, const.ACCOUNTS, None,
                const.VIEW, const.FIELDS
            )
        return data

    async def _step3(data: dict, context, const, pipe, iteration, step_number) -> dict:
        ids = (await rapid_v1.followingids(data['fields']['username'])).get('ids', [])
        has_id = 'User ID' in data['fields']
        my_id = None
        if has_id:
            my_id = {'id_str': data['fields']['User ID']}
        if ids and not has_id:
            my_id = await rapid_v1.users(data['fields']['username'])
            if my_id:
                my_id = my_id[0]
        if ids and my_id:
            id_ = my_id.get('id_str', None)
            if id_:
                if const.PROD:
                    await airtabel.update(const.BASE, const.ACCOUNTS, data['id'], {
                        "Scraping Status": "Followers scraped",
                        "User ID": id_
                    })
                return {
                    'user_data': data,  # airtable bundle
                    'id': id_,  # rapid_id from rapid or airtable
                    'my_id': my_id, # origin rapid_id
                    'ids': ids # list of folower id
                }

        if const.PROD:
            await airtabel.update(const.BASE, const.ACCOUNTS, data['id'], {
                "Scraping Status": "Error"
            })

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> dict:
        objects = []
        ids = set()
        for user in data:
            for follower in user['ids']:
                if follower not in ids and await check_tuser(follower, const):
                    ids.add(follower)
                    await acreate(rapid_id=follower)
                    OUT.print(f'User {follower} was added!')
                    execution_logger.info(f'User {follower} was added!')
                    objects.append({
                        'id': follower,
                        'row_id': user['user_data']['id']
                    })
        return objects

    async def _step8(data: dict, context, const, pipe, iteration, step_number) -> list:
        answer = (await rapid_v2.userbyrestid(data['id']))
        answer = answer.get('data', {}).get('user', {}).get('result', None)
        if answer:
            answer['row_id'] = data['row_id']
        return answer

    async def _step9(data: dict, context, const, pipe, iteration, step_number) -> list:
        if len(data['_urls']) == 0:
            return None
        link_in_bio = data['legacy'].get('entities', {}).get('url', {}).get('urls', [])
        if link_in_bio:
            link_in_bio = link_in_bio[0].get('expanded_url')
        else:
            link_in_bio = None

        profile_picture = data['legacy'].get('profile_image_url_https', None)
        if profile_picture:
            profile_picture = [{'url': profile_picture}]
        else:
            profile_picture = []
        email = data['legacy'].get('description', '') + " " + data['legacy'].get('location', '')
        email_search_line = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        email = re.findall(email_search_line, email)
        if email:
            email = email[0]
        else:
            email = None
        urls = data['legacy'].get('entities', {}).get('url', {}).get('urls', [])
        urls = [item.get('expanded_url') for item in urls]
        return {
            '_urls': urls,
            "User ID": data['rest_id'],
            "username": data['legacy']['name'],
            "Display Name": data['legacy']['name'],
            "Profile Picture": profile_picture,
            "Follower": data['legacy']["followers_count"],
            "Posts": data['legacy']["media_count"],
            "Twitter DMs open": data['legacy']["can_dm"],
            "Bio": data['legacy']["description"],
            "Link in Bio": link_in_bio,
            "E-Mail": email,
            "OnlyFans URL 1": "",
            "OnlyFans URL 2": "",
            # "OnlyFans URL 3": "",
            # "OnlyFans URL 4": "",
            "Scraped from": [data['row_id']]
        }

    async def _step10(data: dict, context, const, pipe, iteration, step_number) -> dict:
        answer = []
        send = False
        for url in LA.sort_links(data['_urls']):
            of, _, lsend = await LA.analize(url)
            send |= lsend
            answer += of
        execution_logger.info(f'''{data["User ID"]}; {data["username"]}
For this links:
{data["_urls"]}
we found this OF links:
{answer}''')
        del data['_urls']
        answer = list(set(list(map(lambda x: x.lower(), answer))))
        if len(answer) > 0:
            data['OnlyFans URL 1'] = answer[0]
        if len(answer) > 1:
            data['OnlyFans URL 2'] = answer[1]
        # if len(answer) > 2:
        #     data['OnlyFans URL 3'] = answer[2]
        # if len(answer) > 3:
        #     data['OnlyFans URL 4'] = answer[3]
        if send:
            return {"fields": data}

    async def _step11(data: list, context, const, pipe, iteration, step_number) -> list:
        _data = []
        for items in data:
            _data += items
        execution_logger.info(f"Total: {len(_data)}")
        return _data

    async def _step12(data: list, context, const, pipe, iteration, step_number) -> list:
        execution_logger.info(f"After filter: {len(data)}")
        answer = list(chunks(data, 10))
        return answer

    async def _step13(data: list, context, const, pipe, iteration, step_number) -> list:
        if const.PROD:
            answer = await airtabel.upsert(
                const.BASE, const.FOLLOWERS, ["User ID"], data
            )
            if 'error' in answer:
                error_logger.error(f"Bad followers upsert: {data}, error: {answer}")
            return answer

    step1 = Step(_step1, name='step_1', const=const)
    step2 = Step(_step2, name='step_2', const=const)
    step3 = Step(_step3, name='step_3', const=const, isloop='aloop', mapper='records')
    step4 = Step(_step4, name='step_4', const=const)
    step8 = Step(_step8, name='step_8', const=const, isloop='aloop')
    step9 = Step(_step9, name='step_9', const=const, isloop='aloop')
    step10 = Step(_step10, name='step_10', const=const, isloop='aloop')
    step12 = Step(_step12, name='step_12', const=const)
    step13 = Step(_step13, name='step_13', const=const, isloop='aloop')

    pipe = step1 > step2
    pipe <= step3
    pipe <= step4
    pipe <= step8
    pipe <= step9
    pipe <= step10
    pipe <= step12
    pipe <= step13

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        OUT.print(f"Hit in cache: {LA.hit_in_cache}")
        OUT.print(f"Processed: {LA.total_processed}; Find OF links: {LA.detected_links}")
        if self.current_loop:
            OUT.print(f"Active [{self.current_loop['name']}] {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")
        self.write_status(self.get_status())

    pipe.set_time_handler(time_handler)
    execution_logger.info(f"Hit in cache: {LA.hit_in_cache}")
    execution_logger.info(f"Processed: {LA.total_processed}; Find OF links: {LA.detected_links}")

    asyncio.run(pipe.run_with_timer(f"Twitter scrap"))
    print('Update state')
    LA.update_state()
    print('End Update state')
