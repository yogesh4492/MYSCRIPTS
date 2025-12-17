import json

def read_json(file):
    with open(file,"r",encoding="utf-8") as read:
        return json.load(read)

def Dump_json(file,data):
    with open(file,"w",encoding="utf-8") as write:
        json.dump(data,write,indent=4,ensure_ascii=False)
