from utils.db_postgres_api import DBPostgresPandas as db
from utils.get_data import get_data as gd
from airflow.hooks.base import BaseHook as bh
from airflow.models.baseoperator import BaseOperator
import pandas as pd
import json, re

class DataToPostgresOperator(BaseOperator):
    methods = {
        "execute",
        "truncate",
        "insert",
        "insert_df_pandas",
        "upsert",
        "create_table_from_df_pandas"
    }
    def __init__(self, 
                 task_id:str, 
                 method:str, 
                 conn_id:str,
                 path_file:str = None,
                 cols_type:dict = None, 
                 table_name:str = None, 
                 range_data:str = None, 
                 step_time:str = None,
                 delimiter:str = ",",
                 encoding:str = None
                 **kwargs):
        super().__init__(task_id = task_id, **kwargs)
        
        self.table_name = table_name
        self.range_data = range_data
        self.method = method
        self.conn_id = conn_id
        self.step_time = step_time
        self.cols_type = cols_type
        self.path_file = path_file
        self.delimiter = delimiter
        self.encoding = encoding
        #self.query = query
        #self.columns = columns
        
        self.maneger = self.__connect()
        
        if method not in self.methods:
            raise Exception(f"The method {method} not implemented")
    
    def __connect(self):
        cred = bh.get_connection(self.conn_id)
        #cred = bh.get_connection("postgres_id")
        login = cred.login
        password = cred.password
        database = json.loads(cred.extra)['database']
        host = cred.host
                
        return db(user = login, password = password, database = database, host = host)
        
    def execute(self, context):
        
        if self.method == "truncate":
            eval(f"self.maneger.truncate(self.table_name)")
        else:
            if self.path_file is None:
                raise Exception("File path is None. Please, put a value in path_file")
            elif self.method == "execute":
                query = self.read_file_sql()
                eval(f"self.maneger.execute(query)")    
            elif self.method == "insert_df_pandas":
                df = self.data_processing()
                eval(f"self.maneger.{self.method}(df = df," +
                    "table = self.table_name,"+
                    "range_data = self.range_data,"+
                    "step_time = self.step_time)")
            elif self.method == "insert":
                data = self.read_file_data()
                columns = self.cols_type.keys()
                eval(f"self.maneger.{self.method}(data = data," +
                    "table = self.table_name,"+
                    "columns = columns, " +
                    "range_data = self.range_data,"+
                    "step_time = self.step_time)")
        self.maneger.close()
    
    def data_processing(self):
        re_string_wrong = re.compile(r"[\?\!\#\@\$]")
        re_num_values = re.compile(r'[0-9\.]')
        # Load data from path file
        if self.cols_type is None:
            df = pd.read_csv(filepath_or_buffer = self.path_file, delimiter = delimiter)
            cols = list(df.columns)
            types = list(df.dtypes)
            self.cols_type = {cols[i]:re.sub(r"\d","",str(types[i])) for i in range(df.shape[1])}
        else:
            data = gd(path_file = self.path_file)
            df = pd.DataFrame(data = data, columns = self.cols_type.keys())
        # Seek for inconsistent data into int columns
        # Seek for inconsistent data into string columns
        columns_fix = []
        for col in self.cols_type:
            if self.cols_type[col] in ("str", "category"):
                test = df[df[col].astype(str).str.match(re_string_wrong)][col]
                if not test.empty:
                    columns_fix.append(col)
                    #print(col)
                    #print(test)
        # Fixin inconsistent string data in a None value            
        def col_fix_str(x, cols):
            for col in cols:
                if re.fullmatch(re_string_wrong, x[col]):
                    x[col] = None
            return x
        df = df.apply(col_fix_str, axis=1, args=(columns_fix, ))
        # Trying convert data values in int or float
        for col in self.cols_type.keys():
            if self.cols_type[col] == "int":
                df[col] = df[col].apply(lambda x: int(x) 
                                        if re.fullmatch(re_num_values, str(x)) 
                                        else None)
            elif self.cols_type[col] == "float":
                df[col] = df[col].apply(lambda x: float(x) 
                                        if re.fullmatch(re_num_values, str(x)) 
                                        else None)
            elif self.cols_type[col] in ("str", "category"):
                df[col] = df[col].apply(lambda x: str(x).strip() 
                                        if not re.fullmatch(re_num_values, x) 
                                        else None)
        # Convert every column
        #df = df.astype(self.cols_type, errors="ignore")
        return df
    # Reading a query string into a file .sql
    def read_file_sql(self):
        with open(self.path_file, "r", encoding="utf-8") as q:
            query = q.read()
        return query
    # Reading a data set from path_file
    def read_file_data(self):
        return gd(self.path_file)
