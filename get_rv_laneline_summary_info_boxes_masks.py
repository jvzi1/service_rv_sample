import os
import pandas as pd
import json
import csv
import random
from tqdm import tqdm
def gb2828(count):
    
    if count >= 2 and count <= 8: # A
        return 2
    elif count >= 9 and count <= 15: # B
        return 3
    elif count >= 16 and count <= 25: # C
        return 5
    elif count >= 26 and count <= 50: # D
        return 8
    elif count >= 51 and count <= 90: # E
        return 13
    elif count >= 91 and count <= 150: # F
        return 20
    elif count >= 151 and count <= 280: # G
        return 32
    elif count >= 281 and count <= 500: # H
        return 50
    elif count >= 501 and count <= 1200: # J
        return 80
    elif count >= 1201 and count <= 3200: # K
        return 125
    elif count >= 3201 and count <= 10000: # L
        return 200
    elif count >= 10001 and count <= 35000: # M
        return 315
    elif count >= 35001 and count <= 150000: # N
        return 500
    elif count >= 150001 and count <= 500000: # P
        return 800
    elif count >= 500001: # Q
        return 1250
    
if __name__ == '__main__':
    # 修改csv地址
    csv_dir = r"D:\data\rhea\rv_laneline\580"
    for csv_path in os.listdir(csv_dir):
        if not csv_path.endswith(".csv"): continue
        batch_id_dict = {}
        data_list = []
        summary_info = {
            "total_frames": 0,
            "all": 0,
            "boxes": 0,
            "masks": 0,
            "sample_frames": 0,
            "sample_nums": 0,
        }
        with open(os.path.join(csv_dir,csv_path),"r") as f:
            count = 0
            data = csv.reader(f)
            for i in data:
                if i[0] == "project_sk":
                    print("skip the first line")
                    continue
                summary_info["total_frames"] += 1
                summary_info["all"] += int(i[7])
                box = i[5]
                mask = i[6]
                summary_info["boxes"] += int(box)
                summary_info["masks"] += int(mask)
                data_list.append(i)
            sample_nums = gb2828(summary_info["all"])
            summary_info["sample_nums"] = sample_nums
            for i in data_list:
                if i[0] == "project_sk":
                    continue
                else:
                    if i[1] not in batch_id_dict:
                        batch_id_dict[i[1]] = [i[3]]
                    else:
                        batch_id_dict[i[1]].append(i[3])
        batch_id_dict_sample = {}
        total_tag = 0
        batch_key = list(batch_id_dict.keys())
        while total_tag < summary_info["sample_nums"]:
            for batch_id in batch_key:
                if batch_id_dict[batch_id]:
                    sample_frame_id = random.choice(batch_id_dict[batch_id]) # 抽取任务id
                    if batch_id not in batch_id_dict_sample:
                        batch_id_dict_sample[batch_id] = [sample_frame_id]
                    else:
                        batch_id_dict_sample[batch_id].append(sample_frame_id)
                    batch_id_dict[batch_id].remove(sample_frame_id)
                    for row in data_list:
                        if row[3] == sample_frame_id:
                            total_tag += int(row[7])
                            summary_info["sample_frames"] += 1
                            break
                if total_tag >= summary_info["sample_nums"]:
                    break
        with open(os.path.join(csv_dir,csv_path.replace(".csv",".json")),"w") as f:
            json.dump(summary_info,f)
        output_json_path = os.path.join(csv_dir, csv_path.replace(".csv", "_batch.json"))
        with open(output_json_path, "w") as f:
            json.dump(batch_id_dict_sample, f, indent=4)
