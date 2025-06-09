import asyncio
import json
import websockets
import time

async def test_websocket_data():
    """Test receiving data from WebSocket and print it."""
    uri = "ws://127.0.0.1:8765"
    
    try:
        print(f"Connecting to {uri}...")
        async with websockets.connect(uri) as websocket:
            print("Connected! Waiting for messages...")
            
            # Receive 10 messages
            for i in range(10):
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    data = json.loads(message)
                    print(f"\nMessage {i+1}:")
                    print(f"  Type: {data['type']}")
                    
                    if data['type'] == 'price':
                        print(f"  Price: {data['price']}")
                    elif data['type'] == 'pnl':
                        print(f"  Grid PnL: {data.get('grid_pnl', 'N/A')}")
                        print(f"  Expo PnL: {data.get('expo_pnl', 'N/A')}")
                        print(f"  Total PnL: {data.get('total_pnl', 'N/A')}")
                    elif data['type'] == 'order':
                        print(f"  Side: {data.get('side', 'N/A')}")
                        print(f"  Price: {data.get('price', 'N/A')}")
                        print(f"  Size: {data.get('size', 'N/A')}")
                    
                    print(f"  Timestamp: {data.get('timestamp', 'N/A')}")
                    
                except asyncio.TimeoutError:
                    print(f"Timeout waiting for message {i+1}")
                    break
                except Exception as e:
                    print(f"Error receiving message {i+1}: {e}")
                    break
                    
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket_data()) 