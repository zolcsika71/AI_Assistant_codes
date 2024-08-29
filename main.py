import logging
import os

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv, find_dotenv
from requests.exceptions import HTTPError



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class EnvManager:
    def __init__(self):
        """Initialize the EnvManager, load the .env file, and store environment variables."""
        self._load_dotenv()
        self.key = None

    @staticmethod
    def _load_dotenv():
        """Load the .env file."""
        dotenv_path = find_dotenv()
        if not dotenv_path:
            raise FileNotFoundError(
                'Could not find the .env file. Please ensure it exists in the project root.'
            )
        load_dotenv(dotenv_path)

    def get_env_variable(self) -> str:
        value = os.getenv(self.key)
        if value is None:
            logging.warning(f'{self.key} is not set in the .env file')
        return value

    def validate_env(self, key: str) -> str:
        """Validate and return the environment variables."""
        self.key = key
        return self.get_env_variable()


class YFinanceService:
    def __init__(self, symbol: str):
        """Initialize the YFinanceService with a symbol."""
        self._symbol = symbol
        self._ticker = yf.Ticker(symbol)
        self.data = None  # Initialize self.data to store historical data
        self.adjusted_close = None  # Store adjusted close prices

    @property
    def symbol(self) -> str:
        """Return the symbol used to initialize the service."""
        return self._symbol

    def get_historical_data(self, period: str = "1mo", interval: str = "1d") -> pd.Series:
        """
        Fetch historical data for the given symbol using yfinance.

        :param period: The period of historical data to fetch. Example values: "1mo", "3mo", "1y", etc.
        :param interval: The data interval. Example values: "1d", "1wk", "1mo", etc.
        :return: A pandas Series of adjusted closing prices indexed by date.
        """
        try:
            logging.info(
                f"Fetching historical data for symbol: {self.symbol} with "
                f"period: {period} and interval: {interval}"
            )
            self.data = self._ticker.history(period=period, interval=interval)
            if self.data.empty:
                logging.error(f"No data found for symbol: {self.symbol}")
                raise HTTPError(f"No data found for symbol: {self.symbol}")
            logging.info(f"Data columns available: {self.data.columns.tolist()}")
            self.adjusted_close = self.calculate_adjusted_close()
            return self.adjusted_close
        except Exception as e:
            logging.error(
                f"Failed to fetch data for symbol: {self.symbol}, error: {e}"
            )
            raise HTTPError(
                f"Failed to fetch data for symbol: {self.symbol}, error: {e}"
            )

    def calculate_adjusted_close(self) -> pd.Series:
        """
        Calculate adjusted close prices using dividend and stock split data.

        :return: A pandas Series of adjusted closing prices.
        """
        if 'Adj Close' in self.data.columns:
            return self.data['Adj Close']
        adjusted_close = self.data['Close'].copy()
        adjustment_factor = (
            self.data['Stock Splits'].replace(0, 1).iloc[::-1].cumprod()
        ).iloc[::-1]
        adjusted_close /= adjustment_factor
        adjusted_close -= self.data['Dividends'].cumsum()
        return adjusted_close

    def calculate_returns(self, return_type: str = "simple") -> pd.Series:
        """
        Calculate returns based on the adjusted close prices.

        :param return_type: Type of return to calculate. Options are "simple" or "log".
        :return: A pandas Series of returns indexed by date.
        """
        if self.adjusted_close is None:
            raise ValueError(
                "Adjusted close prices not calculated. Please call get_historical_data first."
            )
        try:
            if return_type == "simple":
                returns = self.adjusted_close.pct_change()
            elif return_type == "log":
                returns = pd.Series(
                    np.log(self.adjusted_close / self.adjusted_close.shift(1)),
                    name="Log Return"
                )
            else:
                logging.error(f"Invalid return type: {return_type}")
                raise ValueError("Invalid return type. Choose 'simple' or 'log'.")

            logging.info(f"Calculated {return_type} returns for symbol: {self.symbol}")
            return returns.dropna()
        except Exception as e:
            logging.error(
                f"Failed to calculate returns for symbol: {self.symbol}, error: {e}"
            )
            raise


if __name__ == '__main__':
    # Validate the environment variables
    try:
        env_manager = EnvManager()
        alpha_key = env_manager.validate_env('ALPHA_VANTAGE_API_KEY')
        fed_key = env_manager.validate_env('FEDERATION_API_KEY')
        print(f"Alpha Vantage API Key: {alpha_key}")
        print(f"Federation API Key: {fed_key}")
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        exit(1)

    # Instantiate and use YFinanceService
    yfinance_service = YFinanceService(symbol="AAPL")
    try:
        historical_data = yfinance_service.get_historical_data(period="5y", interval="1d")
        print(historical_data)
    except HTTPError as e:
        logging.error(f"An error occurred: {e}")
        exit(1)
