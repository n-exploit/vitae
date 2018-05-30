from steem import Steem
from steem.amount import Amount
from datetime import datetime
import time
import os
import csv
import ast
import requests
import shutil
import rethinkdb as r

def cls():
    os.system('cls')

def value(rshares):
    return (float(rshares) * reward_share * base)

stream = set()
def wish():
    with open("data/execution.log", "w+", newline='') as town_square:
        execution_block = csv.writer(town_square)
        for wish in execution_archive:
            if wish["Hash"] not in stream:
                execution_block.writerow([datetime.utcnow(), wish])  
                stream.add(wish["Hash"])
        town_square.flush() 

def execute(uid, post_time, command):
    ledger_hash = str(uid) + " " + str(command[2]) + " " + str(post_time)
                    
    try:
        cost = costs[command[1]]
        message = "'{} {} {}' Executed Successfully".format(command[0], 
                                                            command[1],
                                                            command[2])
        reconciled.add(ledger_hash)
        
    except:
        cost = 0
        message = "ERROR: Invalid Command {} {}".format(command[0][1])
        reconciled.add(ledger_hash)
    
    executions.append({"Execution Time":str(post_time),
                        "UID":uid,
                        "Command": command[0],
                        "Object": command[1],
                        "Arguments":command[2:],
                        "Cost":cost,
                        "Message":message})
    
def archive(uid, post_time, command):
    
    ledger_hash = str(uid) + " " + str(command[2]) + " " + str(post_time)
                    
    try:
        cost = costs[command[1]]
        message = "'{} {} {}' Executed Successfully".format(command[0], 
                                                            command[1],
                                                            command[2])
        
    except:
        cost = 0
        message = "ERROR: Invalid Command {} {}".format(command[0][1])
    
    execution_archive.append({"Execution Time":str(post_time),
                        "UID":uid,
                        "Command": command[0],
                        "Object": command[1],
                        "Arguments":command[2:],
                        "Cost":cost,
                        "Message":message,
                        "Hash": ledger_hash})

def zeitgeist(date, title):
    if date not in trends:
        trends[date] = {}
        for word in title.split('-'):
            if word not in trends[date]:
                trends[date][word] = 1
            else:
                trends[date][word] += 1
    else:
        for word in title.split('-'):
            if word not in trends[date]:
                trends[date][word] = 1
            else:
                trends[date][word] += 1
                
def download_file(uid, url):
    # NEED TO FIGURE OUT URL HANDLING
    try:
        local_filename = url.split('/')[-1]
        target_folder = ("guests/{}/{}".format(uid, local_filename))
        re = requests.get(url, stream=True)
        with open(target_folder, 'wb') as f:
            shutil.copyfileobj(re.raw, f)
    except:
        pass

    return local_filename
    
def expand(constellations):
    for word in constellations:
        if word not in connections:
            connections[word] = 1
        else:
            connections[word] += 1
            
def generate_datafile(codex, name):
    with open(name, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        x = []
        y = []
        for metric in codex:
            x.append(codex[metric])
            y.append(metric)
        
        writer.writerow(y)
        writer.writerow(x)
            
def generate_logfile(log, name):
    with open(name, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        for row in log:
            try:
                writer.writerow([row, log[row]])
            except:
                pass
            
def generate_postlog(log, name):
    with open(name, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        for row in log:
            try:
                writer.writerow(log[row])
            except UnicodeEncodeError:
                pass

def generate_memberfile(log, name):
    with open(name, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        for row in log:
            i_row = log[row]
            out = []
            out.append(row)
            for i in i_row:
                out.append(i_row[i])
            try:
                writer.writerow(out)
            except:
                pass

def generate_trends(log, name):
    with open(name, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        for row in log:
            topic_count = log[row]
            for topic in topic_count:
                writer.writerow([row, topic, topic_count[topic]])
                
# def save_file(members):
    
r.connect("localhost", 28015).repl()

try:
    r.db("VITAE").table_create("posts").run()
except:
    pass

try:
    r.db("VITAE").table_create("members").run()
except:
    pass

# r.db("VITAE").table("executions").delete().run()
try:
    r.db("VITAE").table_create("executions").run()
except:
    pass

# Variable Instantiation
cls()

s = Steem()

post_archive = []
archive_load = {}

reward_fund =  s.get_reward_fund()
reward_balance = float(reward_fund["reward_balance"].split()[0])
recent_claims = float(reward_fund["recent_claims"])
reward_share = float(reward_balance / recent_claims)
base = float(s.get_current_median_history_price()["base"].split()[0])

post_log = {}
cluster = {}
executions = []
execution_archive = []
reconciled = set()
cluster["metadata"] = {}
metadata = cluster["metadata"]
metadata["posts"] = 0
metadata["dollars"] = 0
metadata["shards"] = 0
metadata["votes"] = 0
metadata["members"] = 0
metadata["weekly_posts"] = 0
costs = {}
members = {}
processed_posts = set()
        
cursor = r.db("VITAE").table("members").run()
for entry in cursor:       
    members[entry["uid"]] = entry
    
cursor = r.db("VITAE").table("executions").run()
for entry in cursor:
    ledger_hash = str(entry["UID"]) + " " + str(entry["Arguments"][0]) + " " + str(entry["Execution Time"])
    reconciled.add(ledger_hash)
        
with open("data/network_costs.dat", "r") as n_costs:
    in_file = csv.reader(n_costs)
    for entity in in_file:
        costs[entity[0]] = int(entity[1])

cluster["connections"] = {}
connections = cluster["connections"] 

cluster["topics"] = {}
trends = cluster["topics"] 

cluster["witness_votes"] = {}
witness_votes = cluster["witness_votes"]
    
execution_count = 0

# Load Users
while True:
    print("Executions: {}".format(execution_count))
    with open("guest.list", "r") as steem_guests:
        in_file = csv.reader(steem_guests)
        for uid_l in in_file:
            uid = uid_l[0]
            print("Processing User: {}".format(uid))
            
            
            # Create User File
            if not os.path.exists("guests/{}".format(uid)):
                os.makedirs("guests/{}".format(uid))
                
            account_data = s.get_account(uid)
            post_count = account_data["post_count"]
            
            while True:
                try:
                    profile = ast.literal_eval(account_data["json_metadata"])["profile"]
                    break
                except:
                    print("Steem DB Connection Failed - Retrying in 5 Seconds...")
                    print("or user [{}] doesn't have a profile".format(uid))
                    time.sleep(5)
                    
            # Record New Members
            if uid not in members:
                members[uid] = {}
                member = members[uid]
                member["create_date"] = str(datetime.utcnow())
                
            # Updating Account Information        
            member = members[uid]
            member["ID"] = account_data["id"]
            member["Balance"] = account_data["balance"]
            member["Reputation"] = account_data["reputation"]
            member["Post Count"] = account_data["post_count"]
            member["Average Bandwidth"] = account_data["average_bandwidth"]
            member["Lifetime Bandwidth"] = account_data["lifetime_bandwidth"]
            member["Average Market Bandwidth"] = account_data["average_market_bandwidth"]
            member["Lifetime Market Bandwidth"] = account_data["lifetime_market_bandwidth"]
            
            metadata["members"] += 1
                            
            for witness in account_data["witness_votes"]:
                if witness not in witness_votes:
                    witness_votes[witness] = 1
                else:
                    witness_votes[witness] += 1
            
            try:
                member["name"] = profile["name"]
            except KeyError:
                pass
            
            try:
                member["about"] = profile["about"]
            except KeyError:
                pass
            
            try:
                member["location"] = profile["location"]
            except KeyError:
                pass
            
            try:
                member["cover_image"] = profile["cover_image"]
            except KeyError:
                pass
            
            try:
                member["profile_image"] = profile["profile_image"]
            except KeyError:
                pass
            
            
            # Querying User Posts
            posts = s.get_blog(uid, post_count, limit=100)
            for post in posts:
                if post["comment"]["id"] in processed_posts:
                    pass
                else:
                    processed_posts.add(post["comment"]["id"])
                    input(post)
                    timestamp = post["comment"]["created"]
                    post_log[timestamp] = []
                    body = str(post["comment"]["body"])
                    post_archive.append(post["comment"])
                        
        #             r.db("VITAE").table("posts").insert([post["comment"]]).run()
                    
                    # Create Team Function ['Team Name', 'Team Channel', 'Team Color']
                    
                    
                    try:
                        commands = body.split(">>>")[1:-1]
                        for command in commands:
                            if ">>" in command:
                                execution = command.split(">>")
                                if len(execution) > 7:
                                    pass
                                else:
                                    ledger_hash = uid + " " + str(execution[2]) + " " + str(timestamp)
                                    if ledger_hash not in reconciled and execution[0] != 'action':
                                        execute(uid, timestamp, execution)
                                    elif execution[0] != 'action':
                                        archive(uid, timestamp, execution)
                            else:
                                pass
        #                 create_team(body.split(">>>")[1:4])
                    except:
                        raise
                    
                    
                    post_data = post_log[timestamp]
                    post_data.append(post["comment"]["created"])
                    post_data.append(post["comment"]["author"])
                    post_data.append(post["comment"]["title"])
                    post_data.append(post["comment"]["category"])
                    post_data.append(value(post["comment"]["net_rshares"]))
                                
                    author = post["comment"]["author"]
                    
                    if author == uid:
                                     
                        # Adding Recent Posts to Organizational Metadata                                                   
                        post_data = post["comment"]
                        metadata["posts"] += 1
                        metadata["shards"] += int(post_data["total_payout_value"][0])
                        metadata["dollars"] += value(post_data["net_rshares"])
                        metadata["votes"] += int(post_data['net_votes'])
                                        
                        if value(post_data["net_rshares"]) > 0:
                            metadata["weekly_posts"] += 1
                        else:
                            pass
                        
                        # Tracking Channel Use                
                        try:
                            tags = ast.literal_eval(post_data["json_metadata"])
                            expand(tags["tags"])
                        except:
                            pass
                        
                        # Looking at Common Words in Titles
                        zeitgeist(post_data["active"].split("T")[0], post_data["permlink"])
                    
    member_list = []
    for i in members:
        members[i]["UID"] = i
        member_list.append(members[i])
        
        
    wish()
    print("------------------")
    print("Network Statistics")
    print("------------------")
    print("last run : {}".format(datetime.utcnow()))
    for i in metadata:
        print("{} : {}".format(i, metadata[i]))
        
                
    r.db("VITAE").table("members").insert(member_list, conflict="replace").run()
    r.db("VITAE").table("posts").insert(post_archive, conflict="replace").run()
    r.db("VITAE").table("executions").insert(executions, conflict="replace").run()
    
    # Generate Datafiles
    generate_datafile(cluster["metadata"], "data/meta.dat")
    generate_logfile(connections, "data/connections.dat")
    generate_postlog(post_log, "data/posts.dat")
    generate_memberfile(members, "data/members.dat")
    generate_trends(trends, "data/trending.dat")
    
    execution_count += 1
    
    time.sleep(120)
    cls()
        


