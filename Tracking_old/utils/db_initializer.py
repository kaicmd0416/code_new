from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, DateTime, text
import yaml
from pathlib import Path
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.exc import OperationalError, SQLAlchemyError


class DatabaseInitializer:
    def __init__(self, config_path=None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'database_config' / 'database_config.yaml'

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except UnicodeDecodeError:
            with open(config_path, 'r', encoding='gbk') as f:
                self.config = yaml.safe_load(f)

        self._setup_logging()
        self._setup_database()

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _setup_database(self):
        try:
            db_config = self.config['database']
            # MySQL 连接字符串
            connection_str = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4"

            self.engine = create_engine(
                connection_str,
                pool_size=db_config.get('pool_size', 5),
                max_overflow=db_config.get('max_overflow', 10),
                pool_pre_ping=True  # 自动检测连接是否有效
            )
            self.metadata = MetaData()

            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                self.logger.info("Database connection successful")

        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            raise

    def create_tables(self):
        """创建所有必要的表"""
        # 创建 realtime_proinfo 表
        Table('realtime_proinfo', self.metadata,
              Column('product_code', String(50), primary_key=True),
              Column('type', String(100), primary_key=True),
              Column('simulation', String(10), primary_key=True),
              Column('value', Float),
              Column('update_time', DateTime)
              )

        # 创建 realtime_futureoptionHolding 表
        Table('realtime_futureoptionholding', self.metadata,
              Column('product_code', String(50), primary_key=True),
              Column('simulation', String(10), primary_key=True),
              Column('code', String(50), primary_key=True),
              Column('direction', String(20), primary_key=True),
              Column('type', String(50)),
              Column('HoldingQty', Float),
              Column('delta', Float),
              Column('mkt_value', Float),
              Column('daily_profit', Float),
              Column('proportion', Float),
              Column('update_time', DateTime)
              )

        # 创建 realtime_holdingChanging 表
        Table('realtime_holdingchanging', self.metadata,
              Column('product_code', String(50), primary_key=True),
              Column('simulation', String(10), primary_key=True),
              Column('code', String(50), primary_key=True),
              Column('direction', String(20), primary_key=True),
              Column('type', String(50), primary_key=True),
              Column('action', String(20)),
              Column('HoldingQty', Float),
              Column('HoldingQty_yes', Float),
              Column('difference', Float),
              Column('update_time', DateTime)

              )
        # 创建 realtime_productStockReturn 表
        Table('realtime_productStockReturn', self.metadata,
              Column('product_name', String(50)),
              Column('portfolio_return_bp', Float),
              Column('excess_return_bp', Float),
              Column('product_code', String(50), primary_key=True),
              Column('update_time', DateTime)

              )
        # 创建 realtime_portfolioReturn 表
        Table('realtime_portfolioReturn', self.metadata,
              Column('score_name', String(50), primary_key=True),
              Column('excess_return_bp', Float),
              Column('portfolio_return_bp', Float),
              Column('update_time', DateTime)
              )

        # 创建 product_detail 表
        Table('product_detail', self.metadata,
              Column('product_code', String(50), primary_key=True),
              Column('product_name', String(50)),
              Column('update_time', DateTime)
              )

        # 创建历史数据表（与实时表结构相同，但增加date字段作为主键的一部分）
        Table('history_proinfo', self.metadata,
              Column('date', DateTime, primary_key=True),
              Column('product_code', String(50), primary_key=True),
              Column('type', String(100), primary_key=True),
              Column('simulation', String(10), primary_key=True),
              Column('value', Float),
              Column('update_time', DateTime)
              )

        Table('history_futureoptionholding', self.metadata,
              Column('date', DateTime, primary_key=True),
              Column('product_code', String(50), primary_key=True),
              Column('simulation', String(10), primary_key=True),
              Column('code', String(50), primary_key=True),
              Column('direction', String(20), primary_key=True),
              Column('type', String(50)),
              Column('HoldingQty', Float),
              Column('delta', Float),
              Column('mkt_value', Float),
              Column('daily_profit', Float),
              Column('proportion', Float),
              Column('update_time', DateTime)
              )

        Table('history_holdingchanging', self.metadata,
              Column('date', DateTime, primary_key=True),
              Column('product_code', String(50), primary_key=True),
              Column('simulation', String(10), primary_key=True),
              Column('code', String(50), primary_key=True),
              Column('direction', String(20), primary_key=True),
              Column('type', String(50), primary_key=True),
              Column('action', String(20)),
              Column('HoldingQty', Float),
              Column('HoldingQty_yes', Float),
              Column('difference', Float),
              Column('update_time', DateTime)
              )

        Table('history_productStockReturn', self.metadata,
              Column('date', DateTime, primary_key=True),
              Column('product_code', String(50), primary_key=True),
              Column('product_name', String(50)),
              Column('portfolio_return_bp', Float),
              Column('excess_return_bp', Float),
              Column('update_time', DateTime)
              )

        Table('history_portfolioReturn', self.metadata,
              Column('date', DateTime, primary_key=True),
              Column('score_name', String(50), primary_key=True),
              Column('excess_return_bp', Float),
              Column('portfolio_return_bp', Float),
              Column('update_time', DateTime)
              )

        # 创建所有表
        self.metadata.create_all(self.engine)