from sqlalchemy import create_engine, text, inspect
import pandas as pd
from datetime import datetime
import yaml
from pathlib import Path
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import numpy as np
import time
from utils.db_initializer import DatabaseInitializer


class DatabaseProcessor:
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

            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                self.logger.info("Database connection successful")

            # 检查并创建必要的表
            self._ensure_tables_exist()

        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            raise
            
    def _ensure_tables_exist(self):
        """检查必要的表是否存在，如果不存在则创建"""
        try:
            # 获取所有表名
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()
            
            # 检查配置中定义的表是否存在
            required_tables = [task_config['table_name'] for task_config in self.config.get('tasks', {}).values()]
            missing_tables = [table for table in required_tables if table.lower() not in [t.lower() for t in existing_tables]]
            
            if missing_tables:
                self.logger.info(f"需要创建的表: {', '.join(missing_tables)}")
                # 使用 DatabaseInitializer 创建表
                initializer = DatabaseInitializer(config_path=Path(__file__).parent.parent / 'database_config' / 'database_config.yaml')
                initializer.create_tables()
                self.logger.info("所有必要的表已创建")
            else:
                self.logger.info("所有必要的表已存在")
                
        except Exception as e:
            self.logger.error(f"检查或创建表时出错: {str(e)}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((OperationalError, SQLAlchemyError))
    )
    def write_to_db(self, df: pd.DataFrame, table_name: str):
        """写入数据到数据库"""
        try:
            # 添加更新时间
            df['update_time'] = datetime.now()


            with self.engine.connect() as conn:
                # 获取表的主键列
                result = conn.execute(text(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"))
                primary_keys = [row[4] for row in result]  # 第4列是列名

            # 构建更新语句
            columns = df.columns.tolist()
            update_cols = [col for col in columns if col not in primary_keys]

            # 构建 INSERT ... ON DUPLICATE KEY UPDATE 语句
            placeholders = ', '.join(['%s'] * len(columns))
            update_str = ', '.join([f"{col}=VALUES({col})" for col in update_cols])

            # 逐条执行插入/更新
            with self.engine.connect() as conn:
                for _, row in df.iterrows():
                    values = tuple(row)
                    conn.execute(
                        text(f"""
                                        INSERT INTO {table_name} ({', '.join(columns)})
                                        VALUES ({placeholders})
                                        ON DUPLICATE KEY UPDATE {update_str}
                                    """),
                        values
                    )
                conn.commit()

            self.logger.info(f"Successfully wrote/updated {len(df)} rows to {table_name}")

        except Exception as e:
            self.logger.error(f"Error writing to {table_name}: {str(e)}")
            raise


    def write_multiple_tables(self, data_dict):
        """
        一次性写入多个表的数据

        Args:
            data_dict: 字典，键为表名，值为对应的DataFrame
        """
        for table_name, df in data_dict.items():
            try:
                # 复制数据框以避免修改原始数据
                df_to_write = df.copy()
                
                # 添加update_time
                df_to_write['update_time'] = datetime.now()
                
                # 处理所有列的NaN值，将其替换为None
                # 首先替换所有的np.nan为None
                df_to_write = df_to_write.replace({np.nan: None})
                
                # 再次检查所有列，确保没有漏掉的NaN
                for col in df_to_write.columns:
                    # 对于浮点数列，再次检查并处理
                    if df_to_write[col].dtype in [float, np.float64]:
                        df_to_write[col] = df_to_write[col].apply(lambda x: None if pd.isna(x) else x)
                    # 对于其他类型列，也再次检查
                    elif df_to_write[col].dtype == 'object':
                        df_to_write[col] = df_to_write[col].apply(lambda x: None if pd.isna(x) else x)
                    # 处理datetime列
                    elif pd.api.types.is_datetime64_any_dtype(df_to_write[col]):
                        df_to_write[col] = df_to_write[col].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else None)
                
                # 获取表的主键列
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"))
                    primary_keys = [row[4] for row in result]  # 第4列是列名
                
                # 处理主键列的空值
                for pk in primary_keys:
                    if pk in df_to_write.columns:
                        # 检查是否有空值
                        null_mask = df_to_write[pk].isnull()
                        if null_mask.any():
                            self.logger.warning(f"表 {table_name} 的主键列 {pk} 存在空值，将替换为'unknown'")
                            # 将空值替换为'unknown'
                            df_to_write.loc[null_mask, pk] = 'unknown'

                # 处理 simulation 列，将 boolean 转换为字符串
                if 'simulation' in df_to_write.columns:
                    df_to_write['simulation'] = df_to_write['simulation'].map({True: 'True', False: 'False'})

                # 构建更新语句
                columns = df_to_write.columns.tolist()
                update_cols = [col for col in columns if col not in primary_keys]

                # 构建 INSERT ... ON DUPLICATE KEY UPDATE 语句
                placeholders = ', '.join([f':{col}' for col in columns])
                update_str = ', '.join([f"{col}=VALUES({col})" for col in update_cols])

                # 逐条执行插入/更新
                with self.engine.connect() as conn:
                    for _, row in df_to_write.iterrows():
                        # 将行数据转换为字典
                        values = row.to_dict()
                        
                        # 再次检查并处理所有的NaN值和时区信息
                        for k, v in list(values.items()):
                            if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
                                values[k] = None
                            elif isinstance(v, pd.Timestamp):
                                values[k] = v.replace(tzinfo=None)
                        
                        conn.execute(
                            text(f"""
                                INSERT INTO {table_name} ({', '.join(columns)})
                                VALUES ({placeholders})
                                ON DUPLICATE KEY UPDATE {update_str}
                            """),
                            values
                        )
                    conn.commit()
                
                self.logger.info(f"Successfully wrote/updated {len(df_to_write)} rows to {table_name}")
                
            except Exception as e:
                self.logger.error(f"Error writing to {table_name}: {str(e)}")
                raise