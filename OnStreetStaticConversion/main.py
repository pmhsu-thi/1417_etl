from datetime import datetime
import logging
from sql_conn import PostgreSQL
import pandas as pd
import os

# 讀取env
def env():
  if os.path.exists('.env'):
    logging.info('成功取得.env')
  else: 
    logging.error('無法取得.env')
    exit()
  for line in open('.env'):
      var = line.strip().split('=')
      if len(var) == 2:
          key, value = var[0].strip(), var[1].strip()
          os.environ[key] = value
 
if __name__ == '__main__':
  now = datetime.now()
  env()
  # 是否為正式環境
  mode = os.environ.get('mode')=="prod"
  # 取得連線
  engine = PostgreSQL().sql_conn()

  # 取得原始資料
  df = pd.read_sql(f"SELECT cell_id, name, day, hour, road_id, road_name FROM public.on_street_static order by grid_id;",con=engine)
  a = df\
    .groupby(['road_id', 'road_name'])\
    .apply(lambda x:  x.to_dict('records'))\
    .to_dict()


  grid_charge_type_statics = []
  road_charge_time_statics = []
  road_charge_time = {}

  for k,v in a.items():
    name_cnt = {}
    grid_charge_type = {}

    # 依照路段id處理
    if k[0] + "0" not in road_charge_time:      
      road_charge_time[k[0]+"0"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':0,'start_time':"-1",'end_time':"-1",'hrs':0}
      road_charge_time[k[0]+"1"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':1,'start_time':"-1",'end_time':"-1",'hrs':0}
      road_charge_time[k[0]+"2"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':2,'start_time':"-1",'end_time':"-1",'hrs':0}
      road_charge_time[k[0]+"3"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':3,'start_time':"-1",'end_time':"-1",'hrs':0}
      road_charge_time[k[0]+"4"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':4,'start_time':"-1",'end_time':"-1",'hrs':0}
      road_charge_time[k[0]+"5"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':5,'start_time':"-1",'end_time':"-1",'hrs':0}
      road_charge_time[k[0]+"6"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':6,'start_time':"-1",'end_time':"-1",'hrs':0}
      road_charge_time[k[0]+"7"] = {'official_id':k[0],'charge_type':k[0]+"-A",'rod_name':k[1],'week_no':7,'start_time':"-1",'end_time':"-1",'hrs':0}
    
    for x in v:
      # 如果id已存在
      grid_charge_type_id = k[0] + "-" + str(int(float(x["cell_id"])))
      if grid_charge_type_id in grid_charge_type:
        # 覆蓋，計數扣掉原計數
        name_cnt[grid_charge_type[grid_charge_type_id]["name_type"]] = name_cnt[grid_charge_type[grid_charge_type_id]["name_type"]] - 1
        if x["name"] in name_cnt:
          name_cnt[x["name"]] = name_cnt[x["name"]] + 1
        else:
          name_cnt[x["name"]] = 1
        
        grid_charge_type[grid_charge_type_id] = {
          'grid_id': grid_charge_type_id,
          'road_id': grid_charge_type_id[:3],
          'charge_type': k[0] + "-A",
          'type_cnt': len(v),
          'name_type': x["name"],
          'name_cnt': -1,
        }        
      else: 
        # 新增且計數
        grid_charge_type[grid_charge_type_id] = {
          'grid_id': grid_charge_type_id,
          'road_id': grid_charge_type_id[:3],
          'charge_type': k[0] + "-A",
          'type_cnt': len(v),
          'name_type': x["name"],
          'name_cnt': -1,
        }
        if x["name"] in name_cnt:
          name_cnt[x["name"]] = name_cnt[x["name"]] + 1
        else:
          name_cnt[x["name"]] = 1

      # 利用字串切割計算小時
      str_list = x["hour"].split("假日", 1)
      if x["day"] == "週一-週五": # 平日
        weekday_list = str_list[0].split("-")
        weekday_start_time_list = weekday_list[0].split(":")
        weekday_start_time = ''.join(weekday_start_time_list)
        weekday_end_time_list = weekday_list[1].split(":")
        weekday_end_time = ''.join(weekday_end_time_list)
        for i in range(0,5):
          road_charge_time[k[0]+str(i)]["start_time"] = weekday_start_time
          road_charge_time[k[0]+str(i)]["end_time"] = weekday_end_time
          road_charge_time[k[0]+str(i)]["hrs"] = int(weekday_end_time_list[0]) - int(weekday_start_time_list[0])
        
      elif x["day"] == "例假日及國定假日": # 假日
        weekend_list = str_list[1].split("-")
        weekend_start_time_list = weekend_list[0].split(":")
        weekend_start_time = ''.join(weekend_start_time_list)
        weekend_end_time_list = weekend_list[1].split(":")
        weekend_end_time = ''.join(weekend_end_time_list)
        for i in range(5,8):
          road_charge_time[k[0]+str(i)]["start_time"] = weekend_start_time
          road_charge_time[k[0]+str(i)]["end_time"] = weekend_end_time
          road_charge_time[k[0]+str(i)]["hrs"] = int(weekend_end_time_list[0]) - int(weekend_start_time_list[0])
        

      elif x["day"] == "週一-週五，例假日及國定假日":        
        if len(str_list) > 1: # 如果是「07:00-20:00 假日08:00-18:00」格式
          weekday_list = str_list[0].split("-")
          weekday_start_time_list = weekday_list[0].split(":")
          weekday_start_time = ''.join(weekday_start_time_list)
          weekday_end_time_list = weekday_list[1].split(":")
          weekday_end_time = ''.join(weekday_end_time_list)
          for i in range(0,5):
            road_charge_time[k[0]+str(i)]["start_time"] = weekday_start_time
            road_charge_time[k[0]+str(i)]["end_time"] = weekday_end_time
            road_charge_time[k[0]+str(i)]["hrs"] = int(weekday_end_time_list[0]) - int(weekday_start_time_list[0])
          weekend_list = str_list[1].split("-")
          weekend_start_time_list = weekend_list[0].split(":")
          weekend_start_time = ''.join(weekend_start_time_list)
          weekend_end_time_list = weekend_list[1].split(":")
          weekend_end_time = ''.join(weekend_end_time_list)
          for i in range(5,8):
            road_charge_time[k[0]+str(i)]["start_time"] = weekend_start_time
            road_charge_time[k[0]+str(i)]["end_time"] = weekend_end_time
            road_charge_time[k[0]+str(i)]["hrs"] = int(weekend_end_time_list[0]) - int(weekend_start_time_list[0])
        else: # 如果是「07:00-20:00」格式
          weekday_list = str_list[0].split("-")
          weekday_start_time_list = weekday_list[0].split(":")
          weekday_start_time = ''.join(weekday_start_time_list)
          weekday_end_time_list = weekday_list[1].split(":")
          weekday_end_time = ''.join(weekday_end_time_list)
          for i in range(0,8):
            road_charge_time[k[0]+str(i)]["start_time"] = weekday_start_time
            road_charge_time[k[0]+str(i)]["end_time"] = weekday_end_time
            road_charge_time[k[0]+str(i)]["hrs"] = int(weekday_end_time_list[0]) - int(weekday_start_time_list[0])
      else:
        logging.warning("例外的day欄位:" + x["day"])

    # 將統計的相同name次數，寫入每一個的name_cnt    
    for v2 in list(grid_charge_type.values()):
      v2['name_cnt'] = name_cnt[v2['name_type']]

    road_charge_time_statics = list(road_charge_time.values())
    grid_charge_type_statics = grid_charge_type_statics + list(grid_charge_type.values())
  

  
  # 轉dataframe
  grid_charge_type_statics_df = pd.DataFrame(grid_charge_type_statics)
  road_charge_time_statics_df = pd.DataFrame(road_charge_time_statics)

  # truncate舊表
  conn = engine.connect()
  print("TRUNCATE grid_charge_type_statics")
  conn.execute("TRUNCATE TABLE grid_charge_type_statics")
  print("TRUNCATE road_charge_time_statics")
  conn.execute("TRUNCATE TABLE road_charge_time_statics")

  grid_charge_type_statics_df.to_sql('grid_charge_type_statics', con=engine, if_exists='append', index=False)
  road_charge_time_statics_df.to_sql('road_charge_time_statics', con=engine, if_exists='append', index=False)