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
        # df = pd.read_csv(os.path.join(r"D:\data\rhea\rv_laneline\580",csv_path))
        # grouped = df.groupby('label_item_sk')
        
        # print(grouped)
        with open(os.path.join(csv_dir,csv_path),"r") as f:
            count = 0
            data = csv.reader(f)
            for i in data:
                if i[0] == "project_sk":
                    continue
                summary_info["total_frames"] += 1
                summary_info["all"] += int(i[7])
                box = i[5]
                mask = i[6]
                summary_info["boxes"] += int(box)
                summary_info["masks"] += int(mask)
                data_list.append(i)
            sample_frames = gb2828(summary_info["total_frames"])
            summary_info["sample_frames"] = sample_frames
            for i in data_list:
                if i[0] == "project_sk":
                    continue
                else:
                    if i[1] not in batch_id_dict:
                        batch_id_dict[i[1]] = [i[3]]
                    else:
                        batch_id_dict[i[1]].append(i[3])
        batch_id_dict_sample = {}
        for batch_id, frame_list in tqdm(batch_id_dict.items()):
            sample_per_batch = sample_frames // len(batch_id_dict)
            if len(frame_list) <= sample_per_batch:
                batch_id_dict_sample[batch_id] = frame_list
            else:
                batch_id_dict_sample[batch_id] = random.sample(frame_list, sample_per_batch)        

        output_json_path = os.path.join(csv_dir, csv_path.replace(".csv", "_batch.json"))
        with open(output_json_path, "w") as f:
            json.dump(batch_id_dict_sample, f, indent=4)
        
        for k,v in batch_id_dict_sample.items():
            frames = 0
            for frame_id in v:
                for row in data_list:
                    if row[3] == frame_id:

                        frames += 1
                        total_tag = row[7]
                        summary_info["sample_nums"] += int(total_tag)
            if frames >= sample_frames:
                break
        with open(os.path.join(csv_dir,csv_path.replace(".csv",".json")),"w") as f:
            json.dump(summary_info,f)


        
        

        