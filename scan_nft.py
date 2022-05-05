from web3 import Web3, HTTPProvider
import requests
from bs4 import BeautifulSoup
import datetime
import threading
import json
import asyncio
import requests
import time
import os
import sys
import ctypes
from http.server import HTTPServer, BaseHTTPRequestHandler
import traceback #ONLY FOR DEBUGGING PURPOSES ONLY
import asyncio
import pandas as pd

def get_tokenmsg(test_address,api):
    tokenName = ''
    tokenSymbol = ''
    url = f'''https://api.etherscan.io/api?module=contract&action=getabi&address={test_address}&apikey={api}'''
    res = requests.get(url=url)
    abi = res.json()['result']
    if (len(abi) > 0) &  (abi != 'Contract source code not verified'):
        getTokenName = w3.eth.contract(address=test_address, abi=abi) #code to get name and symbol from token address
        fun_list = getTokenName.all_functions()
        fun_list = [str(fun) for fun in fun_list]
        fun_list = [fun.split("<Function ")[1].split("(")[0] for fun in fun_list]
        fun_list = [fun.lower() for fun in fun_list]
        name_sum = sum(['name'== fun in fun for fun in fun_list])
        symbol_sum = sum(['symbol'== fun in fun for fun in fun_list])
        if (name_sum > 0) & (symbol_sum > 0):
            tokenName = getTokenName.functions.name().call()
            tokenSymbol = getTokenName.functions.symbol().call()
            return tokenName,tokenSymbol
        else:
            return '',''
    else:
        return '',''
    
    
def get_code_erc721(tokenAddress,api):
    url = "https://api.etherscan.io/api?module=contract&action=getsourcecode&address=%s&apikey=%s"%(tokenAddress,api)
    res = requests.get(url=url)
#     if (len(abi) > 0) &  (abi != 'Contract source code not verified'):
    statu = 0
    try:
        many_code = str(res.json())
        if 'ERC721' in many_code:
            statu = 1
        else:
            statu = 0
    except Exception as e:
        statu = 0
    
    finally:
        return statu
    
    
    
def get_evenfilter(start_block,end_block):
    event_signature_transfer = Web3.keccak(text="OwnershipTransferred(address,address)").hex()
    if end_block == None:
        event_filter = w3.eth.filter({'topics': [event_signature_transfer],
                                      'fromBlock':start_block,
    #                                   'toBlock':14673944
                                     })
    else:
        event_filter = w3.eth.filter({'topics': [event_signature_transfer],
                                      'fromBlock':start_block,
                                      'toBlock':end_block
                                     })
    return event_filter
    
with open('setting.json', 'r') as obj:
    set_json = json.load(obj)
node = set_json['node']
w3 = Web3(Web3.WebsocketProvider(node,websocket_kwargs={'timeout':60}))
api = set_json['ethscan_api']
start_block = set_json['start_block']
end_block_final = set_json['end_block_final']
group_list = list(range(start_block-1,end_block_final+1,500))
if max(group_list) != end_block_final:
    group_list.append(end_block_final)
group_zip = zip(group_list[:-1],group_list[1:])
transfer_events_list = []
for start,end in group_zip:
    print('start,end',(start,end))
    event_filter = get_evenfilter(start,end)
    transfer_events = w3.eth.getFilterLogs(event_filter.filter_id)
    transfer_events_list.append(transfer_events)

transfer_events = w3.eth.getFilterLogs(event_filter.filter_id)
hash_list = []
address_list = []
blockNumber_list = []
transactionIndex_list = []
topic0_list = []
topic1_list = []
topic2_list = []
tokenName_list = []
tokenSymbol_list = []
is_erc721_list = []
print("len(transfer_events)",len(transfer_events))
address_all_msg = {}

for transfer_events in transfer_events_list:
    print("transfer_events",transfer_events)
    for i in range(len(transfer_events)):
        print("transfer_events_%s/%s"%(i,len(transfer_events)))
        hash_per = transfer_events[i]['transactionHash'].hex()
        address_per = transfer_events[i]['address']
        blockNumber_per = transfer_events[i]['blockNumber']
        transactionIndex_per = transfer_events[i]['transactionIndex']
        topics_zero = ''
        topics_one = ''
        topics_two = ''
        top_len = len(transfer_events[i]['topics'])
        if top_len >= 3:
        
            topics_zero = transfer_events[i]['topics'][0].hex()
            topics_one = transfer_events[i]['topics'][1].hex()
            topics_two = transfer_events[i]['topics'][2].hex()
        elif top_len == 2:
            topics_zero = transfer_events[i]['topics'][0].hex()
            topics_one = transfer_events[i]['topics'][1].hex()
        elif top_len == 1:
            topics_zero = transfer_events[i]['topics'][0].hex()
        else:
            pass
        hash_list.append(hash_per)
        address_list.append(address_per)
        blockNumber_list.append(blockNumber_per)
        transactionIndex_list.append(transactionIndex_per)
        topic0_list.append(topics_zero)
        topic1_list.append(topics_one)
        topic2_list.append(topics_two)
        tokenName,tokenSymbol = get_tokenmsg(address_per,api)
        is_erc721 = get_code_erc721(address_per,api)
        tokenName_list.append(tokenName)
        tokenSymbol_list.append(tokenSymbol)
        is_erc721_list.append(is_erc721)
        if is_erc721 == 1:
            print("="*30)
            tokenName_new = tokenName.replace("/","")
            f_txt = open("./scan/nft_scan_%s_%s.txt"%(hash_per,tokenName_new),'w',encoding='utf-8')
            print("address_per",address_per)
            print("tokenSymbol",tokenSymbol)
            msg = f'''hash_per:{hash_per},\naddress_per:{address_per},\nblockNumber_per:{blockNumber_per},\ntransactionIndex_per:{transactionIndex_per},\ntopics_zero:{topics_zero},\ntopics_one:{topics_one},\ntopics_two:{topics_two},\ntokenName:{tokenName},\ntokenSymbol:{tokenSymbol},\nis_erc721:{is_erc721},\n
            '''
            f_txt.writelines(msg + "\n")
            print("写入成功")
            f_txt.close()   
        
scan_df = pd.DataFrame()
scan_df['hash'] = hash_list
scan_df['address'] = address_list
scan_df['block_no'] = blockNumber_list
scan_df['tra_index'] = transactionIndex_list
scan_df['topic0'] = topic0_list
scan_df['topic1'] = topic1_list
scan_df['topic2'] = topic2_list
scan_df['tokenName'] = tokenName_list
scan_df['tokenSymbol'] = tokenSymbol_list
scan_df['is_erc721'] = is_erc721_list
scan_df.to_csv("scan_df_%s-%s.csv"%(start_block,end_block_final),index=False)
