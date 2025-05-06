import os
import json
import asyncio
import websockets
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from mcp.server.fastmcp import FastMCP
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame

# Initialize FastMCP server
mcp = FastMCP("alpaca-trading")

# Initialize Alpaca clients using environment variables
API_KEY = os.getenv("API_KEY_ID")
API_SECRET = os.getenv("API_SECRET_KEY")

# Check if keys are available
if not API_KEY or not API_SECRET:
    raise ValueError("Alpaca API credentials not found in environment variables.")

# Initialize trading and data clients
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
stock_client = StockHistoricalDataClient(API_KEY, API_SECRET)

# WebSocket connection manager
class AlpacaWebSocketManager:
    """
    Manages WebSocket connections to Alpaca's streaming API.
    """
    def __init__(self):
        self.connections = {}  # Map of session_id to websocket connection
        self.subscriptions = {}  # Map of session_id to set of symbols
        self.ws_url = "wss://stream.data.alpaca.markets/v2/iex"
        self.paper_ws_url = "wss://stream.data.sandbox.alpaca.markets/v2/iex"
        self.trade_updates_url = "wss://api.alpaca.markets/stream"
        self.paper_trade_updates_url = "wss://paper-api.alpaca.markets/stream"
        self.running = True
        self.handlers = {}  # Map of session_id to handler function

    async def connect(self, session_id: str, handler, paper: bool = True):
        """
        Connect to Alpaca's WebSocket API.
        
        Args:
            session_id: Unique ID for this connection
            handler: Callback function to handle incoming messages
            paper: Whether to use the paper trading environment
        """
        if session_id in self.connections:
            await self.disconnect(session_id)
            
        self.handlers[session_id] = handler
        self.subscriptions[session_id] = set()
        
        url = self.paper_ws_url if paper else self.ws_url
        
        try:
            websocket = await websockets.connect(url)
            self.connections[session_id] = websocket
            
            # Authenticate
            auth_msg = {
                "action": "auth",
                "key": API_KEY,
                "secret": API_SECRET
            }
            await websocket.send(json.dumps(auth_msg))
            response = await websocket.recv()
            print(f"Auth response: {response}")
            
            # Start listener task
            asyncio.create_task(self._listener(session_id))
            
            return True
        except Exception as e:
            print(f"WebSocket connection error: {e}")
            return False
    
    async def disconnect(self, session_id: str):
        """Disconnect from Alpaca's WebSocket API."""
        if session_id in self.connections:
            try:
                await self.connections[session_id].close()
            except Exception as e:
                print(f"Error closing WebSocket: {e}")
            finally:
                del self.connections[session_id]
                if session_id in self.subscriptions:
                    del self.subscriptions[session_id]
                if session_id in self.handlers:
                    del self.handlers[session_id]
    
    async def subscribe(self, session_id: str, symbols: List[str]):
        """Subscribe to symbols for the specified session."""
        if session_id not in self.connections:
            return False
        
        websocket = self.connections[session_id]
        
        # Add the symbols to our subscription set
        new_symbols = set(symbols) - self.subscriptions[session_id]
        if not new_symbols:
            return True  # Already subscribed
        
        self.subscriptions[session_id].update(new_symbols)
        
        # Send subscription message
        sub_msg = {
            "action": "subscribe",
            "trades": list(new_symbols),
            "quotes": list(new_symbols),
            "bars": list(new_symbols)
        }
        
        try:
            await websocket.send(json.dumps(sub_msg))
            return True
        except Exception as e:
            print(f"WebSocket subscription error: {e}")
            return False
    
    async def unsubscribe(self, session_id: str, symbols: List[str]):
        """Unsubscribe from symbols for the specified session."""
        if session_id not in self.connections:
            return False
        
        websocket = self.connections[session_id]
        
        # Remove the symbols from our subscription set
        symbols_to_unsub = set(symbols) & self.subscriptions[session_id]
        if not symbols_to_unsub:
            return True  # Not subscribed to these symbols
        
        self.subscriptions[session_id] -= symbols_to_unsub
        
        # Send unsubscription message
        unsub_msg = {
            "action": "unsubscribe",
            "trades": list(symbols_to_unsub),
            "quotes": list(symbols_to_unsub),
            "bars": list(symbols_to_unsub)
        }
        
        try:
            await websocket.send(json.dumps(unsub_msg))
            return True
        except Exception as e:
            print(f"WebSocket unsubscription error: {e}")
            return False
    
    async def _listener(self, session_id: str):
        """Listen for incoming messages from the WebSocket."""
        if session_id not in self.connections:
            return
        
        websocket = self.connections[session_id]
        handler = self.handlers[session_id]
        
        try:
            while self.running:
                message = await websocket.recv()
                data = json.loads(message)
                
                # Process the message
                if isinstance(data, list):
                    for item in data:
                        if 'T' in item:
                            msg_type = item['T']
                            
                            # Parse different message types
                            if msg_type == 't':  # Trade
                                await handler('trade', item)
                            elif msg_type == 'q':  # Quote
                                await handler('quote', item)
                            elif msg_type == 'b':  # Bar/Agg
                                await handler('bar', item)
                            elif msg_type == 'error':
                                print(f"WebSocket error: {item}")
                            elif msg_type in ('success', 'subscription'):
                                # Control messages, can be logged or ignored
                                pass
                            else:
                                print(f"Unknown message type: {msg_type}")
        except websockets.exceptions.ConnectionClosed:
            print(f"WebSocket connection closed for session {session_id}")
        except Exception as e:
            print(f"WebSocket listener error: {e}")
        finally:
            # Try to reconnect
            if session_id in self.connections:
                del self.connections[session_id]
                
                # Attempt to reconnect in 5 seconds
                await asyncio.sleep(5)
                if self.running:
                    # Get the symbols we were subscribed to
                    symbols = list(self.subscriptions.get(session_id, set()))
                    if session_id in self.handlers:
                        handler = self.handlers[session_id]
                        # Reconnect and resubscribe
                        if await self.connect(session_id, handler):
                            if symbols:
                                await self.subscribe(session_id, symbols)

# Create the WebSocket manager
ws_manager = AlpacaWebSocketManager()

# Create a second WebSocket manager for trade updates
class TradeUpdatesWebSocketManager:
    """
    Manages WebSocket connections to Alpaca's trade updates streaming API.
    """
    def __init__(self):
        self.connections = {}  # Map of session_id to websocket connection
        self.ws_url = "wss://api.alpaca.markets/stream"
        self.paper_ws_url = "wss://paper-api.alpaca.markets/stream"
        self.running = True
        self.handlers = {}  # Map of session_id to handler function
    
    async def connect(self, session_id: str, handler, paper: bool = True):
        """
        Connect to Alpaca's trade updates WebSocket API.
        
        Args:
            session_id: Unique ID for this connection
            handler: Callback function to handle incoming messages
            paper: Whether to use the paper trading environment
        """
        if session_id in self.connections:
            await self.disconnect(session_id)
            
        self.handlers[session_id] = handler
        
        url = self.paper_ws_url if paper else self.ws_url
        
        try:
            websocket = await websockets.connect(url)
            self.connections[session_id] = websocket
            
            # Authenticate
            auth_msg = {
                "action": "authenticate",
                "data": {
                    "key_id": API_KEY,
                    "secret_key": API_SECRET
                }
            }
            await websocket.send(json.dumps(auth_msg))
            response = await websocket.recv()
            print(f"Trade updates auth response: {response}")
            
            # Subscribe to trade updates
            sub_msg = {
                "action": "listen",
                "data": {
                    "streams": ["trade_updates"]
                }
            }
            await websocket.send(json.dumps(sub_msg))
            response = await websocket.recv()
            print(f"Trade updates subscription response: {response}")
            
            # Start listener task
            asyncio.create_task(self._listener(session_id))
            
            return True
        except Exception as e:
            print(f"Trade updates WebSocket connection error: {e}")
            return False
    
    async def disconnect(self, session_id: str):
        """Disconnect from Alpaca's WebSocket API."""
        if session_id in self.connections:
            try:
                await self.connections[session_id].close()
            except Exception as e:
                print(f"Error closing trade updates WebSocket: {e}")
            finally:
                del self.connections[session_id]
                if session_id in self.handlers:
                    del self.handlers[session_id]
    
    async def _listener(self, session_id: str):
        """Listen for incoming messages from the WebSocket."""
        if session_id not in self.connections:
            return
        
        websocket = self.connections[session_id]
        handler = self.handlers[session_id]
        
        try:
            while self.running:
                message = await websocket.recv()
                data = json.loads(message)
                
                # Process the message if it's a trade update
                if 'stream' in data and data['stream'] == 'trade_updates':
                    await handler(data)
        except websockets.exceptions.ConnectionClosed:
            print(f"Trade updates WebSocket connection closed for session {session_id}")
        except Exception as e:
            print(f"Trade updates WebSocket listener error: {e}")

# Create the trade updates WebSocket manager
trade_updates_manager = TradeUpdatesWebSocketManager()

# Account information tools
@mcp.tool()
async def get_account_info() -> str:
    """Get the current account information including balances and status."""
    account = trading_client.get_account()
    
    info = f"""
Account Information:
-------------------
Account ID: {account.id}
Status: {account.status}
Currency: {account.currency}
Buying Power: ${float(account.buying_power):.2f}
Cash: ${float(account.cash):.2f}
Portfolio Value: ${float(account.portfolio_value):.2f}
Equity: ${float(account.equity):.2f}
Long Market Value: ${float(account.long_market_value):.2f}
Short Market Value: ${float(account.short_market_value):.2f}
Pattern Day Trader: {'Yes' if account.pattern_day_trader else 'No'}
Day Trades Remaining: {account.daytrade_count if hasattr(account, 'daytrade_count') else 'Unknown'}
"""
    return info

@mcp.tool()
async def get_positions() -> str:
    """Get all current positions in the portfolio."""
    positions = trading_client.get_all_positions()
    
    if not positions:
        return "No open positions found."
    
    result = "Current Positions:\n-------------------\n"
    for position in positions:
        result += f"""
Symbol: {position.symbol}
Quantity: {position.qty} shares
Market Value: ${float(position.market_value):.2f}
Average Entry Price: ${float(position.avg_entry_price):.2f}
Current Price: ${float(position.current_price):.2f}
Unrealized P/L: ${float(position.unrealized_pl):.2f} ({float(position.unrealized_plpc) * 100:.2f}%)
-------------------
"""
    return result

# Market data tools
@mcp.tool()
async def get_stock_quote(symbol: str) -> str:
    """
    Get the latest quote for a stock.
    
    Args:
        symbol: Stock ticker symbol (e.g., AAPL, MSFT)
    """
    try:
        request_params = StockLatestQuoteRequest(symbol_or_symbols=symbol)
        quotes = stock_client.get_stock_latest_quote(request_params)
        
        if symbol in quotes:
            quote = quotes[symbol]
            return f"""
Latest Quote for {symbol}:
------------------------
Ask Price: ${quote.ask_price:.2f}
Bid Price: ${quote.bid_price:.2f}
Ask Size: {quote.ask_size}
Bid Size: {quote.bid_size}
Timestamp: {quote.timestamp}
"""
        else:
            return f"No quote data found for {symbol}."
    except Exception as e:
        return f"Error fetching quote for {symbol}: {str(e)}"

@mcp.tool()
async def get_stock_bars(symbol: str, days: int = 5) -> str:
    """
    Get historical price bars for a stock.
    
    Args:
        symbol: Stock ticker symbol (e.g., AAPL, MSFT)
        days: Number of trading days to look back (default: 5)
    """
    try:
        # Calculate start time based on days
        start_time = datetime.now() - timedelta(days=days)
        
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Day,
            start=start_time
        )
        
        bars = stock_client.get_stock_bars(request_params)
        
        if symbol in bars and bars[symbol]:
            result = f"Historical Data for {symbol} (Last {days} trading days):\n"
            result += "---------------------------------------------------\n"
            
            for bar in bars[symbol]:
                result += f"Date: {bar.timestamp.date()}, Open: ${bar.open:.2f}, High: ${bar.high:.2f}, Low: ${bar.low:.2f}, Close: ${bar.close:.2f}, Volume: {bar.volume}\n"
            
            return result
        else:
            return f"No historical data found for {symbol} in the last {days} days."
    except Exception as e:
        return f"Error fetching historical data for {symbol}: {str(e)}"

@mcp.tool()
async def get_stock_bars_intraday(symbol: str, timeframe: str = "1Min", start: str = None, limit: int = 50) -> dict:
    """
    Get historical intraday price bars for a stock.
    
    Args:
        symbol: Stock ticker symbol (e.g., AAPL, MSFT)
        timeframe: Timeframe for the bars (e.g., 1Min, 5Min, 15Min)
        start: Start datetime in ISO format (default: 1 day ago)
        limit: Maximum number of bars to return (default: 50)
    """
    try:
        # Calculate start time if not provided
        if not start:
            start_time = datetime.now() - timedelta(days=1)
        else:
            start_time = datetime.fromisoformat(start.replace('Z', '+00:00'))
        
        # Convert timeframe string to TimeFrame enum
        if timeframe == "1Min":
            tf = TimeFrame.Minute
        elif timeframe == "5Min":
            tf = TimeFrame.Minute * 5
        elif timeframe == "15Min":
            tf = TimeFrame.Minute * 15
        elif timeframe == "Hour" or timeframe == "1Hour":
            tf = TimeFrame.Hour
        else:
            tf = TimeFrame.Minute  # Default to 1 minute
        
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=start_time,
            limit=limit
        )
        
        bars = stock_client.get_stock_bars(request_params)
        
        # Convert to serializable format
        result = {}
        if symbol in bars:
            result[symbol] = []
            for bar in bars[symbol]:
                result[symbol].append({
                    "timestamp": bar.timestamp.isoformat(),
                    "open": float(bar.open),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "close": float(bar.close),
                    "volume": int(bar.volume)
                })
        
        return result
    except Exception as e:
        return {"error": f"Error fetching intraday data for {symbol}: {str(e)}"}

# WebSocket streaming tools
@mcp.tool()
async def start_websocket_stream(session_id: str, symbols: List[str], paper: bool = True) -> Dict[str, Any]:
    """
    Start a WebSocket stream for real-time market data.
    
    Args:
        session_id: Unique ID for this streaming session
        symbols: List of stock ticker symbols to subscribe to
        paper: Whether to use the paper trading environment
    """
    async def message_handler(message_type, message):
        # Convert message to a format compatible with our MarketBar model
        if message_type == 'bar':
            # For bar/agg messages
            return {
                "event_type": "bar",
                "data": {
                    "timestamp": message.get('t'),
                    "symbol": message.get('S'),
                    "open": float(message.get('o', 0)),
                    "high": float(message.get('h', 0)),
                    "low": float(message.get('l', 0)),
                    "close": float(message.get('c', 0)),
                    "volume": float(message.get('v', 0))
                }
            }
        elif message_type == 'quote':
            # For quote messages
            return {
                "event_type": "quote",
                "data": {
                    "timestamp": message.get('t'),
                    "symbol": message.get('S'),
                    "bid_price": float(message.get('bp', 0)),
                    "ask_price": float(message.get('ap', 0)),
                    "bid_size": float(message.get('bs', 0)),
                    "ask_size": float(message.get('as', 0))
                }
            }
        elif message_type == 'trade':
            # For trade messages
            return {
                "event_type": "trade",
                "data": {
                    "timestamp": message.get('t'),
                    "symbol": message.get('S'),
                    "price": float(message.get('p', 0)),
                    "size": float(message.get('s', 0))
                }
            }
    
    # Set the message handler to push updates via SSE
    ws_handler = mcp.create_sse_push_handler(message_handler)
    
    # Connect to Alpaca's WebSocket API
    success = await ws_manager.connect(session_id, ws_handler, paper)
    
    if success and symbols:
        # Subscribe to the specified symbols
        await ws_manager.subscribe(session_id, symbols)
    
    return {
        "session_id": session_id,
        "status": "connected" if success else "failed",
        "subscribed_symbols": list(ws_manager.subscriptions.get(session_id, set()))
    }

@mcp.tool()
async def stop_websocket_stream(session_id: str) -> Dict[str, Any]:
    """
    Stop a WebSocket stream.
    
    Args:
        session_id: Unique ID for the streaming session to stop
    """
    await ws_manager.disconnect(session_id)
    
    return {
        "session_id": session_id,
        "status": "disconnected"
    }

@mcp.tool()
async def subscribe_websocket_symbols(session_id: str, symbols: List[str]) -> Dict[str, Any]:
    """
    Subscribe to additional symbols on an active WebSocket stream.
    
    Args:
        session_id: Unique ID for the streaming session
        symbols: List of stock ticker symbols to subscribe to
    """
    if session_id not in ws_manager.connections:
        return {
            "session_id": session_id,
            "status": "not_connected",
            "error": "No active WebSocket connection for this session ID"
        }
    
    success = await ws_manager.subscribe(session_id, symbols)
    
    return {
        "session_id": session_id,
        "status": "subscribed" if success else "failed",
        "subscribed_symbols": list(ws_manager.subscriptions.get(session_id, set()))
    }

@mcp.tool()
async def unsubscribe_websocket_symbols(session_id: str, symbols: List[str]) -> Dict[str, Any]:
    """
    Unsubscribe from symbols on an active WebSocket stream.
    
    Args:
        session_id: Unique ID for the streaming session
        symbols: List of stock ticker symbols to unsubscribe from
    """
    if session_id not in ws_manager.connections:
        return {
            "session_id": session_id,
            "status": "not_connected",
            "error": "No active WebSocket connection for this session ID"
        }
    
    success = await ws_manager.unsubscribe(session_id, symbols)
    
    return {
        "session_id": session_id,
        "status": "unsubscribed" if success else "failed",
        "subscribed_symbols": list(ws_manager.subscriptions.get(session_id, set()))
    }

# Order management tools
@mcp.tool()
async def get_orders(status: str = "all", limit: int = 10) -> str:
    """
    Get orders with the specified status.
    
    Args:
        status: Order status to filter by (open, closed, all)
        limit: Maximum number of orders to return (default: 10)
    """
    try:
        # Convert status string to enum
        if status.lower() == "open":
            query_status = QueryOrderStatus.OPEN
        elif status.lower() == "closed":
            query_status = QueryOrderStatus.CLOSED
        else:
            query_status = QueryOrderStatus.ALL
            
        request_params = GetOrdersRequest(
            status=query_status,
            limit=limit
        )
        
        orders = trading_client.get_orders(request_params)
        
        if not orders:
            return f"No {status} orders found."
        
        result = f"{status.capitalize()} Orders (Last {len(orders)}):\n"
        result += "-----------------------------------\n"
        
        for order in orders:
            result += f"""
Symbol: {order.symbol}
ID: {order.id}
Type: {order.type}
Side: {order.side}
Quantity: {order.qty}
Status: {order.status}
Submitted At: {order.submitted_at}
"""
            if hasattr(order, 'filled_at') and order.filled_at:
                result += f"Filled At: {order.filled_at}\n"
                
            if hasattr(order, 'filled_avg_price') and order.filled_avg_price:
                result += f"Filled Price: ${float(order.filled_avg_price):.2f}\n"
                
            result += "-----------------------------------\n"
            
        return result
    except Exception as e:
        return f"Error fetching orders: {str(e)}"

@mcp.tool()
async def place_market_order(symbol: str, side: str, quantity: float) -> str:
    """
    Place a market order.
    
    Args:
        symbol: Stock ticker symbol (e.g., AAPL, MSFT)
        side: Order side (buy or sell)
        quantity: Number of shares to buy or sell
    """
    try:
        # Convert side string to enum
        if side.lower() == "buy":
            order_side = OrderSide.BUY
        elif side.lower() == "sell":
            order_side = OrderSide.SELL
        else:
            return f"Invalid order side: {side}. Must be 'buy' or 'sell'."
        
        # Create market order request
        order_data = MarketOrderRequest(
            symbol=symbol,
            qty=quantity,
            side=order_side,
            time_in_force=TimeInForce.DAY
        )
        
        # Submit order
        order = trading_client.submit_order(order_data)
        
        return f"""
Market Order Placed Successfully:
--------------------------------
Order ID: {order.id}
Symbol: {order.symbol}
Side: {order.side}
Quantity: {order.qty}
Type: {order.type}
Time In Force: {order.time_in_force}
Status: {order.status}
"""
    except Exception as e:
        return f"Error placing market order: {str(e)}"

@mcp.tool()
async def place_limit_order(symbol: str, side: str, quantity: float, limit_price: float) -> str:
    """
    Place a limit order.
    
    Args:
        symbol: Stock ticker symbol (e.g., AAPL, MSFT)
        side: Order side (buy or sell)
        quantity: Number of shares to buy or sell
        limit_price: Limit price for the order
    """
    try:
        # Convert side string to enum
        if side.lower() == "buy":
            order_side = OrderSide.BUY
        elif side.lower() == "sell":
            order_side = OrderSide.SELL
        else:
            return f"Invalid order side: {side}. Must be 'buy' or 'sell'."
        
        # Create limit order request
        order_data = LimitOrderRequest(
            symbol=symbol,
            qty=quantity,
            side=order_side,
            time_in_force=TimeInForce.DAY,
            limit_price=limit_price
        )
        
        # Submit order
        order = trading_client.submit_order(order_data)
        
        return f"""
Limit Order Placed Successfully:
-------------------------------
Order ID: {order.id}
Symbol: {order.symbol}
Side: {order.side}
Quantity: {order.qty}
Type: {order.type}
Limit Price: ${float(order.limit_price):.2f}
Time In Force: {order.time_in_force}
Status: {order.status}
"""
    except Exception as e:
        return f"Error placing limit order: {str(e)}"

@mcp.tool()
async def cancel_all_orders() -> str:
    """Cancel all open orders."""
    try:
        cancel_statuses = trading_client.cancel_orders()
        return f"Successfully canceled all open orders. Status: {cancel_statuses}"
    except Exception as e:
        return f"Error canceling orders: {str(e)}"

# Account management tools
@mcp.tool()
async def close_all_positions(cancel_orders: bool = True) -> str:
    """
    Close all open positions.
    
    Args:
        cancel_orders: Whether to cancel all open orders before closing positions (default: True)
    """
    try:
        trading_client.close_all_positions(cancel_orders=cancel_orders)
        return "Successfully closed all positions."
    except Exception as e:
        return f"Error closing positions: {str(e)}"

@mcp.tool()
async def start_trade_updates_stream(session_id: str, paper: bool = True) -> Dict[str, Any]:
    """
    Start a WebSocket stream for real-time trade updates.
    
    Args:
        session_id: Unique ID for this streaming session
        paper: Whether to use the paper trading environment
    
    Returns:
        Dict with connection status and session_id
    """
    async def message_handler(message):
        # Extract the trade update data
        if 'data' in message and 'order' in message['data']:
            order_data = message['data']['order']
            event_type = message['data'].get('event')
            
            # Format the data to be consistent with our expected format
            return {
                "event_type": "order_update",
                "data": {
                    "id": order_data.get('id'),
                    "client_order_id": order_data.get('client_order_id'),
                    "symbol": order_data.get('symbol'),
                    "side": order_data.get('side'),
                    "type": order_data.get('type'),
                    "status": order_data.get('status'),
                    "updated_at": order_data.get('updated_at'),
                    "created_at": order_data.get('created_at'),
                    "filled_qty": order_data.get('filled_qty'),
                    "qty": order_data.get('qty'),
                    "filled_avg_price": order_data.get('filled_avg_price'),
                    "event": event_type
                }
            }
    
    # Set the message handler to push updates via SSE
    ws_handler = mcp.create_sse_push_handler(message_handler)
    
    # Connect to Alpaca's trade updates WebSocket API
    success = await trade_updates_manager.connect(session_id, ws_handler, paper)
    
    return {
        "session_id": session_id,
        "status": "connected" if success else "failed"
    }

# Run the server
if __name__ == "__main__":
    mcp.run(transport='sse')