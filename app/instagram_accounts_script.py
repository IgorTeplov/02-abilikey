import asyncio

from libs.api import AirtableApi, RapidApi, _request_counter
from libs.settings import AIRTABLE_TOKEN, XRapidAPIHost, XRapidAPIKey
from datetime import datetime
from libs.logs import error_logger
from libs.makepipeline import Step, Const, OUT
from libs.utils import chunks

const = Const(
    BASE='Instagram',
    ACCOUNTS='IG Accounts',
    ACCOUNTS_VIEW="Account - To be updated",
    USER_COUNT=None,
    FIELDS=[
        "Username", "Follower", "Created", "Account ID", "IG Reels",
        "Follower (T-1)", "Total Account Views (Started: 01.08.2024)",
        "Total Account Views (T-1)"
    ]
)

airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN
)
rapid = RapidApi(
    "https://instagram-scraper-20252.p.rapidapi.com/v1/",
    {"X-RapidAPI-Key": XRapidAPIKey, "X-RapidAPI-Host": XRapidAPIHost}
)

if __name__ == '__main__':

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

    async def _step3(data: dict, context, const, pipe, iteration, step_number) -> dict:
        id_ = data['id']
        username = data.get('fields', {}).get('Username', None)
        if username:
            info = await asyncio.gather(
                rapid.info(username, id_),
                rapid.highlights(username, id_)
            )
            return {
                'info': info[0],
                'highlights': info[1],
                'username': username,
                'Follower': data['fields'].get('Follower', 0),
                'Total Account Views': data['fields']['Total Account Views (Started: 01.08.2024)'],
                'id': id_
            }

    async def _step4(data: dict, context, const, pipe, iteration, step_number) -> dict:
        update = {}
        info = data['info']["answer"].get("data", {})
        highlights = data['highlights']["answer"].get("data", {})
        update["Status"] = "Potential Ban"
        if len(info.keys()) > 0:
            update["Status"] = "Active"
            update["Privacy"] = "Private"
            if info["follower_count"] != 0:
                update["Follower"] = info["follower_count"]
            else:
                error_logger.error(
                    f"Zero follower Error: {data['username']} {info['follower_count']}"
                )
            update["Account - Last Scraped"] = datetime.today().strftime(
                "%Y-%m-%d"
            )
            if data['Follower'] != 0:
                update["Total Account Views (T-1)"] = data['Follower']
            update["Follower (T-1)"] = data['Total Account Views']
            update["Following"] = info["following_count"]
            update["Posts"] = info["media_count"]
            update["Display Name"] = info["full_name"]
            update["Bio"] = info["biography"]
            if len(info["bio_links"]) > 0:
                update["URL in Bio"] = info["bio_links"][0].get("url", "")
            update["Account ID"] = info["id"]
            if not info["is_private"]:
                update["Privacy"] = "Public"

        if len(highlights.keys()) > 0:
            update["Account - Last Scraped"] = datetime.today().strftime(
                "%Y-%m-%d"
            )
            update["Status"] = "Active"
            total_highlights = len(highlights.get("items", []))
            total_media = sum(list(map(
                lambda item: item["media_count"],
                highlights.get("items", [])
            )))

            update["Highlights"] = total_highlights
            update["Highlights - total media"] = total_media

        return {
            'id': data['id'],
            'fields': update
        }

    async def _step5(data: list, context, const, pipe, iteration, step_number) -> list:
        return list(chunks(data, 10))

    async def _step6(data: list, context, const, pipe, iteration, step_number) -> dict:
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

    pipe = step1 > step2
    pipe <= step3
    pipe <= step4
    pipe <= step5
    pipe <= step6

    async def time_handler(self, elapsed):
        started = _request_counter["started"]
        ended = _request_counter["ended"]

        OUT.print(f"Requests: {started}/{ended}")
        if self.current_loop:
            OUT.print(f"Active {self.current_loop['active']}, {self.current_loop['finished']}/{self.current_loop['max']}")
        OUT.print(f"{datetime.now().strftime('%H:%M:%S')} - {elapsed}")

    pipe.set_time_handler(time_handler)

    asyncio.run(pipe.run_with_timer("Instagram Accounts"))
