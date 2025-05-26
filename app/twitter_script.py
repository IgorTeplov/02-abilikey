import asyncio
from libs.api import AirtableApi
from libs.settings import AIRTABLE_TOKEN_Twitter
from libs.makepipeline import Step, Const, OUT
from libs.logs import error_logger, execution_logger

from sys import argv


airtabel = AirtableApi(
    "https://api.airtable.com/v0/", AIRTABLE_TOKEN_Twitter
)

const = Const(
    BASE='Twitter E-Mail Scraping Automation',
    ACCOUNTS='TikTok Accounts',
    ACCOUNTS_VIEW=' '.join(argv[1:]),
    VIDEOS='TikTok Videos',
    IG_ACCOUNTS='Instagram Accounts',
    USER_COUNT=None,
    POST_COUNT=80,
    MARK=True
)

if __name__ == '__main__':

    async def _step1(data: None, context, const, pipe, iteration, step_number) -> None:
        # Init Airtable
        OUT.print(await airtabel.bases())

    async def _step2(data: None, context, const, pipe, iteration, step_number) -> list:
        # Get Accounts
        return data

    step1 = Step(_step1, name='step_1', const=const)
    step2 = Step(_step2, name='step_2', const=const)

    pipe = step1 > step2

    asyncio.run(pipe.run(f"Twitter scrap"))
