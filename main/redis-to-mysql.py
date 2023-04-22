import redis
import json
import datetime
import pymysql

# 连接 Redis 数据库
r = redis.Redis(host='your redis', port=6379, password="123456", db=15)

# 连接 MySQL 数据库
db = pymysql.connect(host='localhost', port=3306, user='root', password='123456', db='qxdatabase')

stationCode = input("请输入气象站编号：")
qxDate = input("请输入需要获取的气象数据日期：")

# 获取有序集合中的所有数据
data = r.zrange(stationCode + '_data_' + qxDate, 0, -1, withscores=True)


# 转换时间戳为东八区时间
def convert_to_beijing_time(timestamp):
    utc_time = datetime.datetime.utcfromtimestamp(timestamp)
    beijing_time = utc_time + datetime.timedelta(hours=8)
    return beijing_time.strftime('%Y-%m-%d %H:%M:%S')


# 构建数据的字典形式
data_dict = []
for item in data:
    value = json.loads(item[0])
    score = item[1]
    data_dict.append({
        "station": stationCode,
        "date": qxDate,
        "datetime": convert_to_beijing_time(score),
        "time": value[0],
        "temperature": value[1],
        "humidity": value[2],
        "speed": value[3],
        "direction": value[4],
        "rain": value[5],
        "sunlight": value[6],
        "pm25": value[7],
        "pm10": value[8]
    })
data_json = json.dumps(data_dict)

# 查询是否存在 station_weather_year 表
year = qxDate.split('-')[0]
cursor = db.cursor()
table_exists = False
sql = "SHOW TABLES LIKE %s"
table_name = f"{stationCode}_weather_{year}"
cursor.execute(sql, (table_name,))
result = cursor.fetchone()

if result:
    table_exists = True
    print("Table exist")
# 如果 station_weather_year 表不存在，则创建该表
if not table_exists:
    sql = """
        CREATE TABLE {station}_weather_{year} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            station VARCHAR(255),
            date DATE,
            datetime DATETIME,
            time TIME,
            temperature FLOAT,
            humidity FLOAT,
            speed FLOAT,
            direction VARCHAR(255),
            rain FLOAT,
            sunlight FLOAT,
            pm25 FLOAT,
            pm10 FLOAT,
            INDEX index_station_date (station, date, datetime, time)
        )
    """.format(station=stationCode, year=year)
    cursor.execute(sql)
    db.commit()

# 查询表中是否已经存在相同日期的数据
sql = "SELECT * FROM {station}_weather_{year} WHERE date = '{date}' LIMIT 1".format(station=stationCode, year=year, date=qxDate)
cursor.execute(sql)
sqlResult = cursor.fetchone()
if sqlResult:
    print("Data exist")
else:
    # 插入数据到 weather_year 表中
    sql = """
        INSERT INTO {station}_weather_{year} (
            station, date, datetime, time, temperature,
            humidity, speed, direction, rain, sunlight, pm25, pm10
        ) VALUES (
            %(station)s, %(date)s, %(datetime)s, %(time)s, %(temperature)s,
            %(humidity)s, %(speed)s, %(direction)s, %(rain)s, %(sunlight)s, %(pm25)s, %(pm10)s
        )
    """.format(station=stationCode, year=year)
    try:
        cursor.executemany(sql, data_dict)
        db.commit()
        print("commit over")
    except Exception as e:
        db.rollback()
        raise e

# 关闭数据库连接
db.close()
