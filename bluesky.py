#!/usr/bin/env python3
import asyncio
import websockets
import json

TRACKED_TERMS = [
    # Macro / Economics
    "inflation", "recession", "federal reserve", "interest rates", "cpi", "powell", 
    # Crypto / Tech
    "bitcoin", "btc", "ethereum", "crypto", "nvidia", "openai",
    # Politics / Geo-Politics
    "trump", "election", "senate", "tariff", "china",
    # Market Specific (Meta)
    "polymarket", "kalshi", "prediction market"
]

async def listen():
    url = 'wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post'
    async with websockets.connect(url) as ws:
        print("Connected!")
        while True:
            message = await ws.recv()
            data = json.loads(message)
            if data['kind'] == 'commit' and data['commit']['operation'] == 'create':
                text = data['commit']['record']['text']
                for term in TRACKED_TERMS:
                    if term in text:
                        print(f"Found term '{term}' in post:")
                        text = text.lower()
                        print(text)
                        print(f"Posted at: {data['commit']['record']['createdAt']}")
                        print("--------------------------------")
                        break

asyncio.run(listen())
