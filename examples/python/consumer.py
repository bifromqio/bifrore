import asyncio

from bifrore import BifroRE


async def main():
    async with BifroRE(
        "./rule.json",
        host="127.0.0.1",
        port=1883,
        client_count=1,
    ) as stream:
        async for message in stream:
            print(f"rule_id={message.rule_id} destinations={message.destinations}")


if __name__ == "__main__":
    asyncio.run(main())
