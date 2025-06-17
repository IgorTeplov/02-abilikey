import asyncio
import libs.init

from libs.api import AirtableApi, _request_counter
from libs.settings import AIRTABLE_TOKEN_Twitter
from libs.utils import chunks
from datetime import datetime
from libs.makepipeline import Step, Const, OUT
from cache.models import TwitterUser
from asgiref.sync import sync_to_async

const = Const(
    BASE='Twitter E-Mail Scraping Automation',
    FOLLOWERS='Scraped Followers',
    FOLLOWERS_VIEW="1. All Accounts",
    ACCOUNTS='Twitter Profiles',
    ACCOUNTS_VIEW="1. All Accounts",
    FIELDS=["username", "User ID"]
)

airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN_Twitter
)

if __name__ == '__main__':

    async def _step1(data: None, context, const, pipe, iteration, step_number) -> None:
        airtabel.load_cache("airtable_twitter_db.json")
        await airtabel.init(const.BASE)
        airtabel.dump_cache("airtable_twitter_db.json")

    async def _step2(data: None, context, const, pipe, iteration, step_number) -> dict:
        accounts = await airtabel.search_until(
            const.BASE, const.ACCOUNTS, None, const.ACCOUNTS_VIEW,
            const.FIELDS
        )
        followers = await airtabel.search_until(
            const.BASE, const.FOLLOWERS, None, const.FOLLOWERS_VIEW,
            const.FIELDS
        )
        return [accounts, followers]

    async def _step3(data: dict, context, const, pipe, iteration, step_number) -> list:
        accounts, followers = data
        for_create_accounts = []
        for_create_followers = []
        duplicates = set()
        for account in accounts['records']:
            id_ = account['fields'].get('User ID', '0')
            if id_ != '0':
                if account['fields']['User ID'] not in duplicates:
                    duplicates.add(account['fields']['User ID'])
                    for_create_accounts.append({
                        'rapid_id': id_,
                        'base': const.BASE,
                        'table': const.ACCOUNTS,
                    })
        for follower in followers['records']:
            id_ = follower['fields'].get('User ID', '0')
            if id_ != '0':
                if follower['fields']['User ID'] not in duplicates:
                    duplicates.add(follower['fields']['User ID'])
                    for_create_followers.append({
                        'rapid_id': id_,
                        'base': const.BASE,
                        'table': const.FOLLOWERS,
                    })
        del duplicates
        return [for_create_accounts, for_create_followers]

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> list:
        accounts, followers = data
        accounts_tasks = []
        followers_tasks = []
        for accounts_ in list(chunks(accounts, 20)):
            batch = []
            for account in accounts_:
                batch.append(TwitterUser(**account))
            accounts_tasks.append(batch)
        for followers_ in list(chunks(followers, 20)):
            batch = []
            for follower in followers_:
                batch.append(TwitterUser(**follower))
            followers_tasks.append(batch)

        async_create = sync_to_async(
            TwitterUser.objects.bulk_create, thread_sensitive=True
        )
        tasks = []
        for reels in accounts_tasks:
            tasks.append(async_create(reels))
        for sounds in followers_tasks:
            tasks.append(async_create(sounds))
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
    pipe.set_exec('7650e821-f21b-4587-b7fa-c4a4657a0553', ['step_2'])

    asyncio.run(pipe.run_with_timer("Make Twitter Cache"))
