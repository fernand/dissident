import api_config

from alpaca.trading.client import TradingClient

client = TradingClient(api_config.ALPACA_PAPER_API_KEY, api_config.ALPACA_PAPER_API_SECRET, paper=True)
# client = TradingClient(api_config.ALPACA_API_KEY, api_config.ALPACA_API_SECRET, paper=False)

resp = client.close_all_positions()

print(resp)