import asyncio
import websockets
import json

async def test_websocket():
    try:
        async with websockets.connect('ws://127.0.0.1:8765') as websocket:
            print("Connected to WebSocket server")
            
            # Try to receive a few messages
            for i in range(3):
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    print(f"Message {i+1}: {data['type']} - {data}")
                except asyncio.TimeoutError:
                    print(f"Timeout waiting for message {i+1}")
                    break
                    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket()) 