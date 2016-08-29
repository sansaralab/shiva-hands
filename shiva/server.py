import asyncio
from json import loads
import requests
from aiobeanstalk.exceptions import TimedOut
from aiobeanstalk.proto import Client
from shiva.common import settings
from shiva.domain.types import UserVisit, UserData


async def main():
    loop = asyncio.get_event_loop()
    client = await Client.connect(settings.BEANSTALKD_HOST, settings.BEANSTALKD_PORT)
    await client.send_command('use', settings.BEANSTALKD_TUBE)
    while True:
        res = await client.send_command('reserve-with-timeout', settings.BEANSTALKD_DELAY)
        if isinstance(res, TimedOut):
            await asyncio.sleep(1)
            continue
        if isinstance(res, Exception):
            raise res

        print("Got job ", res.job_id)

        try:
            data = UserVisit(**loads(res.data.decode('utf-8')))
        except TypeError:
            data = UserData(**loads(res.data.decode('utf-8')))

        if isinstance(data, UserVisit):
            fut = loop.run_in_executor(None, requests.post,
                                       '%s:%d/api/v1/track_visit'.format(settings.CORE_HOST, settings.CORE_PORT), data._asdict())
            resp = await fut
            print(resp.text)

        if isinstance(data, UserData):
            fut = loop.run_in_executor(None, requests.post,
                                       '%s:%d/api/v1/add_data'.format(settings.CORE_HOST, settings.CORE_PORT), data._asdict())
            resp = await fut
            print(resp.text)

    client.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
