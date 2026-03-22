import asyncio

from bifrore import BifroRE


async def main():
    async with BifroRE(
        "./rule.json",
        host="127.0.0.1",
        port=1883,
        client_count=1,
    ) as stream:
        async for rule_index, payload, destinations in stream:
            print(f"rule_index={rule_index} destinations={destinations} payload={payload.decode('utf-8', errors='replace')}")


if __name__ == "__main__":
    asyncio.run(main())
