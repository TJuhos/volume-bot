import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import asyncio
import json
import websockets
import time
import threading
from queue import Queue, Empty
import logging

# Configure logging
logging.getLogger('websockets').setLevel(logging.WARNING)

# Page config
st.set_page_config(page_title="Volume Bot Dashboard", layout="wide")

# Force fresh session state
def init_fresh_session():
    keys_to_init = ['price_data', 'pnl_data', 'connection_status', 'last_update', 'websocket_started', 'debug_messages', 'data_processed_count']
    
    st.session_state.price_data = []
    st.session_state.pnl_data = {}
    st.session_state.connection_status = "Disconnected"
    st.session_state.last_update = 0
    st.session_state.websocket_started = False
    st.session_state.debug_messages = []
    st.session_state.data_processed_count = 0

# Always initialize fresh
init_fresh_session()

# Create a fresh global queue each time
data_queue = Queue()

def websocket_worker():
    """WebSocket client worker that runs in a separate thread."""
    def debug_print(msg):
        """Print debug message to console and queue."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] [WS] {msg}")
        # Add debug message to queue for main thread to process
        try:
            data_queue.put({"type": "debug", "message": f"[{timestamp}] {msg}"}, timeout=1)
        except:
            pass  # Ignore queue full errors
    
    try:
        debug_print("Starting WebSocket worker thread")
        
        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def connect_and_listen():
            uri = "ws://127.0.0.1:8765"
            retry_count = 0
            max_retries = 5
            
            while retry_count < max_retries:
                try:
                    debug_print(f"Connecting to {uri} (attempt {retry_count + 1})")
                    
                    async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as websocket:
                        debug_print("Connected!")
                        data_queue.put({"type": "status", "status": "Connected"})
                        retry_count = 0
                        
                        while True:
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                                data = json.loads(message)
                                data['received_at'] = time.time()
                                
                                # Put data in queue
                                try:
                                    data_queue.put(data, timeout=1)
                                    debug_print(f"Queued {data['type']} message")
                                except:
                                    debug_print("Queue full, skipping message")
                                    
                            except asyncio.TimeoutError:
                                debug_print("Timeout, sending ping...")
                                try:
                                    await websocket.ping()
                                except:
                                    debug_print("Ping failed")
                                    break
                                    
                            except websockets.exceptions.ConnectionClosed:
                                debug_print("Connection closed")
                                break
                                
                            except Exception as e:
                                debug_print(f"Receive error: {e}")
                                break
                                
                except Exception as e:
                    retry_count += 1
                    debug_print(f"Connection failed: {e}")
                    data_queue.put({"type": "status", "status": f"Error: {str(e)}"})
                    
                    if retry_count < max_retries:
                        await asyncio.sleep(2 ** retry_count)
                    else:
                        debug_print("Max retries reached")
                        return

        # Run the connection logic
        loop.run_until_complete(connect_and_listen())
        
    except Exception as e:
        debug_print(f"Worker error: {e}")
    finally:
        debug_print("Worker ended")

def start_websocket():
    """Start WebSocket client if not already running."""
    if not st.session_state.websocket_started:
        print("[MAIN] Starting WebSocket...")
        thread = threading.Thread(target=websocket_worker, daemon=True)
        thread.start()
        st.session_state.websocket_started = True

def process_data():
    """Process all pending data from the queue."""
    processed = 0
    messages_processed = []
    
    # Process up to 50 messages at once
    while processed < 50:
        try:
            data = data_queue.get_nowait()
            processed += 1
            messages_processed.append(data['type'])
            
            if data['type'] == 'status':
                old_status = st.session_state.connection_status
                st.session_state.connection_status = data['status']
                if old_status != data['status']:
                    st.session_state.debug_messages.append(f"Status: {old_status} -> {data['status']}")
                
            elif data['type'] == 'debug':
                st.session_state.debug_messages.append(data['message'])
                # Keep only last 15 debug messages
                if len(st.session_state.debug_messages) > 15:
                    st.session_state.debug_messages = st.session_state.debug_messages[-15:]
                
            elif data['type'] == 'price':
                # Add price data
                price_point = {
                    'timestamp': datetime.fromtimestamp(data.get('timestamp', time.time())),
                    'price': data['price']
                }
                st.session_state.price_data.append(price_point)
                
                # Keep only last 50 price points for performance
                if len(st.session_state.price_data) > 50:
                    st.session_state.price_data = st.session_state.price_data[-50:]
                    
                # Update last update time
                st.session_state.last_update = time.time()
                    
            elif data['type'] == 'pnl':
                # Update PnL data
                st.session_state.pnl_data = {
                    'grid_pnl': data.get('grid_pnl', 0),
                    'expo_pnl': data.get('expo_pnl', 0),
                    'total_pnl': data.get('total_pnl', 0),
                    'timestamp': datetime.fromtimestamp(data.get('timestamp', time.time()))
                }
                
                # Update last update time
                st.session_state.last_update = time.time()
            
        except Empty:
            break
        except Exception as e:
            print(f"[MAIN] Error processing data: {e}")
            break
    
    if processed > 0:
        st.session_state.data_processed_count += processed
        print(f"[MAIN] Processed {processed} messages: {messages_processed}")
    
    return processed > 0

def main():
    st.title("🤖 Volume Bot Dashboard")
    
    # Start WebSocket connection
    start_websocket()
    
    # Process any new data
    data_updated = process_data()
    
    # Show processing stats
    st.sidebar.write(f"**Data processed:** {st.session_state.data_processed_count}")
    st.sidebar.write(f"**Queue size:** {data_queue.qsize()}")
    
    # Status indicator
    status = st.session_state.connection_status
    if status == "Connected":
        st.success(f"✅ Connected")
    else:
        st.error(f"❌ {status}")
    
    # Current data counts
    price_count = len(st.session_state.price_data)
    has_pnl = bool(st.session_state.pnl_data)
    last_update = st.session_state.last_update
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Price Points", price_count)
    with col2:
        st.metric("PnL Data", "✅ Yes" if has_pnl else "❌ No")
    with col3:
        if last_update > 0:
            seconds_ago = int(time.time() - last_update)
            st.metric("Last Update", f"{seconds_ago}s ago")
        else:
            st.metric("Last Update", "Never")
    
    # PnL Metrics - FORCE DISPLAY
    st.subheader("💰 PnL Metrics")
    
    if st.session_state.pnl_data:
        grid_pnl = st.session_state.pnl_data.get('grid_pnl', 0)
        expo_pnl = st.session_state.pnl_data.get('expo_pnl', 0)
        total_pnl = st.session_state.pnl_data.get('total_pnl', 0)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Grid PnL", f"${grid_pnl:.4f}", f"{grid_pnl:.4f}")
        with col2:
            st.metric("Exposure PnL", f"${expo_pnl:.2f}", f"{expo_pnl:.2f}")
        with col3:
            st.metric("Total PnL", f"${total_pnl:.2f}", f"{total_pnl:.2f}")
        
        # Show timestamp
        pnl_time = st.session_state.pnl_data.get('timestamp')
        if pnl_time:
            st.write(f"*Last PnL update: {pnl_time.strftime('%H:%M:%S')}*")
    else:
        st.warning("No PnL data received yet")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Grid PnL", "$0.00")
        with col2:
            st.metric("Exposure PnL", "$0.00")
        with col3:
            st.metric("Total PnL", "$0.00")
    
    # Price Chart
    st.subheader("📈 Price Chart")
    if st.session_state.price_data and len(st.session_state.price_data) > 0:
        df = pd.DataFrame(st.session_state.price_data)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['price'],
            mode='lines+markers',
            name='Bitcoin Price',
            line=dict(color='#FF6B35', width=2),
            marker=dict(size=4)
        ))
        
        fig.update_layout(
            title="Bitcoin Price (Live)",
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            height=400,
            template="plotly_dark"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        st.write(f"*Showing {len(st.session_state.price_data)} price points*")
    else:
        st.info("📊 Waiting for price data...")
        if st.session_state.connection_status == "Connected":
            st.write("Connected but no price data yet...")
        
    # Debug info in sidebar
    st.sidebar.subheader("🐛 Debug Messages")
    if st.session_state.debug_messages:
        for msg in st.session_state.debug_messages[-8:]:  # Show last 8 messages
            st.sidebar.text(msg)
    else:
        st.sidebar.write("No debug messages yet")
    
    # Raw data inspection
    with st.expander("🔍 Raw Data Inspection"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Latest Price Data:**")
            if st.session_state.price_data:
                latest_prices = st.session_state.price_data[-3:]  # Last 3 prices
                for i, price_point in enumerate(latest_prices):
                    st.write(f"{i+1}. ${price_point['price']:.2f} at {price_point['timestamp'].strftime('%H:%M:%S')}")
            else:
                st.write("No price data")
        
        with col2:
            st.write("**PnL Data:**")
            if st.session_state.pnl_data:
                st.json(st.session_state.pnl_data, expanded=False)
            else:
                st.write("No PnL data")
    
    # Force refresh button
    if st.button("🔄 Force Refresh"):
        st.rerun()
    
    # Auto refresh logic - AGGRESSIVE REFRESH
    if data_updated or data_queue.qsize() > 0:
        # If we processed data or there's more data waiting, refresh immediately
        time.sleep(0.1)
        st.rerun()
    elif status == "Connected":
        # If connected but no new data, refresh every 2 seconds
        time.sleep(2)
        st.rerun()
    else:
        # If disconnected, refresh every 1 second to show connection attempts
        time.sleep(1)
        st.rerun()

if __name__ == "__main__":
    main() 