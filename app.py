import os

os.environ.setdefault("PNTS_API_DATA_SOURCE", "published")
os.environ.setdefault("PNTS_PUBLIC_SAFE_MODE", "true")
os.environ.setdefault("PNTS_PUBLIC_DELAY_MINUTES", "1440")

from pre_news_trading_surveillance.api.app import app
