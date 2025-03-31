from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from airflow.providers.microsoft.azure.sensors.eventhub import AzureEventHubSensor
from airflow.providers.microsoft.azure.hooks.eventhub import AzureEventHubHook
import joblib
import sqlite3


#SQLtite Database Path
DB_PATH = "/opt/airflow/data/airflow_storage.db"
TABLE_NAME = "FLIGHT_REALTIME"
# initialize EventHub
#暂时用的是我的EVENTHUB string
EVENTHUB_CONNECTION_STR = "Endpoint=sb://bigdataeventhub.servicebus.windows.net/;SharedAccessKeyName=flighteventhub;SharedAccessKey=xBbkGCvZHDeJcjcXDI05QgEYjGYEpoHol+AEhNyW+p8=;EntityPath=flight_eventhub"

EVENTHUB_NAME = "flight_eventhub"

# 1.  Ingest Data from Azure Event Hub and Store in SQLite
def ingest_from_eventhub():
    hook = AzureEventHubHook(eventhub_conn_id='azure_eventhub_default', eventhub_name=EVENTHUB_NAME)
    messages = hook.get_conn().receive()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT,
            message TEXT
        )
    """)

    for msg in messages:
        cursor.execute(f"INSERT INTO {TABLE_NAME} (event_time, message) VALUES (?, ?)",
                       (datetime.now(), msg.body_as_str()))

    conn.commit()
    conn.close()


# 2. data process
'''

问题🤔：我们的realtime data其实已经很干净，不确定是否还需要清洗

'''
def preprocess_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        f"SELECT * FROM {TABLE_NAME}", conn)
    '''这是个SQL命令，选取需要的列，要保证和训练模型用的特征一样'''

    conn.close()

    df.dropna(inplace=True)
    df.to_csv('/opt/airflow/data/preprocessed_data.csv', index=False)


def fine_tune_model():
    df = pd.read_csv('/opt/airflow/data/preprocessed_data.csv')
    train_df = df.sample(frac=0.8, random_state=42)
    test_df = df.drop(train_df.index)

    X_train, y_train = train_df.drop(columns=['label']), train_df['label']

    model = joblib.load('BATCH MODEL PATH ') # 训练好的模型路径

    model.fit(X_train, y_train)
    joblib.dump(model, '/opt/airflow/model/classifier.pkl') #保存fine-tune的模型



#获取一条最新的实时数据进行预测他的延迟
API_URL = "https://api.example.com/data"  # 替换成实际 API 地址

'''
🤔
问题: 我们finetune的数据来源和这个数据来源是一样的：AviationAPI， 模型已经学习了这个信息，会不会overfit？

'''

def fetch_data_from_api():
    """从 API 获取数据并存入 SQLite 数据库"""
    API_URL = "https://api.aviationstack.com/v1/flights?access_key=15cb385363a4a32008e2c3f7597e5f14&limit=100&flight_status=active&limit=1"
    # 只拿一条最新的数据 所以👆上面代码最后 limit=1
    HEADERS = {
        'Accept': 'application/json',
        'Accept-Version': 'v1'
    }

    response = requests.get(API_URL)
    '''
    1.不确定是否需要存到SQLite，或者直接放入模型预测
    2.如果不用存到DB， 那可以把这一步融入到下一步 predict（）进去
    '''
    if response.status_code == 200:
        data = response.json()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time TEXT,
                message TEXT
            )
        """) # 这是一个SQL语句，需要修改！！！

        # 假设 API 返回 JSON 对象，包含 "message" 字段
        cursor.execute(f"INSERT INTO {TABLE_NAME} (event_time, message) VALUES (?, ?)",
                       (datetime.now(), json.dumps(data)))

        conn.commit()
        conn.close()
        print("✅ API data has been successfully stored in SQLite！")
    else:
        print(f"❌ API error，code: {response.status_code}")

#Make Predictions by new model and Store in SQLite
def predict():
    model = joblib.load('/opt/airflow/model/classifier.pkl')
    new_data = "新的realtime data" # 可能需要重新编写一个
    pred = model.predict(new_data)
    prob = model.predict_proba(new_data)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    '''SQL命令， 用于存储到sqlite'''
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            prediction INTEGER,
            confidence REAL
        )
    """)
    cursor.execute("INSERT INTO predictions (timestamp, prediction, confidence) VALUES (?, ?, ?)",
                   (datetime.now(), pred[0], prob.max()))

    conn.commit()
    conn.close()


#DAG file
#用来把以上定义的函数，组成管道
with DAG('ml_pipeline',
         start_date=datetime(2024, 3, 25),
         schedule_interval='@daily',
         catchup=False) as dag:
    #第一步
    task_ingest = PythonOperator(task_id='ingest_from_eventhub', python_callable=ingest_from_eventhub)
    #第二步
    task_preprocess = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data)
    #第三步
    task_fine_tune = PythonOperator(task_id='fine_tune_model', python_callable=fine_tune_model)
    #第四步

    #第四步
    task_predict = PythonOperator(task_id='predict', python_callable=predict)

    # Define task dependencies
    task_ingest >> task_preprocess >> task_fine_tune >> task_predict