import sys, types, time, os
os.environ.update(BOT_TOKEN="x", GROUP_ID="@Promoter0007", MONGODB_URI="mongodb://fake", OWNER_ID="111", KEEPALIVE_SECONDS="0", AUTO_SET_WEBHOOK="0")

# ---- stub flask / pymongo if missing
try:
    import flask
except ImportError:
    f = types.ModuleType("flask")
    class Flask:
        def __init__(s,*a,**k): pass
        def get(s,*a,**k): return lambda fn: fn
        post = get
    f.Flask=Flask; f.request=None; f.jsonify=lambda *a,**k:None
    sys.modules["flask"]=f

class Cur(list):
    pass
class Coll:
    def __init__(s): s.docs=[]
    def create_index(s,*a,**k): pass
    def _m(s,d,q):
        for k,v in q.items():
            if isinstance(v,dict) and "$in" in v:
                if d.get(k) not in v["$in"]: return False
            elif d.get(k)!=v: return False
        return True
    def find_one(s,q,proj=None):
        for d in s.docs:
            if s._m(d,q): return d
    def find(s,q,proj=None): return [d for d in s.docs if s._m(d,q)]
    def insert_one(s,d):
        if any(x["chat_id"]==d["chat_id"] and x.get("referred_id")==d.get("referred_id") for x in s.docs):
            class DuplicateKeyError(Exception): pass
            raise DuplicateKeyError()
        s.docs.append(d)
    def update_one(s,q,u,upsert=False):
        d=s.find_one(q)
        if d is None and upsert:
            d=dict(q); d.update(u.get("$setOnInsert",{})); s.docs.append(d)
        elif d is not None: d.update(u.get("$set",{}))
    def count_documents(s,q): return len(s.find(q))
    def aggregate(s,p):
        cnt={}; 
        for d in s.docs:
            if d["chat_id"]==p[0]["$match"]["chat_id"]:
                cnt.setdefault(d["inviter_id"],[0,d["inviter_name"]]); cnt[d["inviter_id"]][0]+=1
        rows=[{"_id":k,"count":v[0],"fallback":v[1]} for k,v in cnt.items()]
        rows.sort(key=lambda r:(-r["count"],r["_id"])); return rows[:p[-1]["$limit"]]
class DB:
    def __init__(s): s.c={}
    def __getattr__(s,n): return s.c.setdefault(n,Coll())
class Client:
    def __init__(s,*a,**k): s.d={}
    def __getitem__(s,n): return s.d.setdefault(n,DB())
    admin=types.SimpleNamespace(command=lambda *a,**k:None)
pm=types.ModuleType("pymongo"); pm.MongoClient=Client; pm.ASCENDING=1; pm.DESCENDING=-1
sys.modules["pymongo"]=pm

sys.path.insert(0,".")
import main as m

calls=[]; admins={222}
def fake_tg(method, payload=None):
    payload=payload or {}
    calls.append((method,payload))
    if method=="getChatMember": 
        return {"ok":True,"result":{"status":"administrator" if payload["user_id"] in admins else "member"}}
    if method=="createChatInviteLink": return {"ok":True,"result":{"invite_link":"https://t.me/+abc%d"%len(calls)}}
    if method=="sendMessage": return {"ok":True,"result":{"message_id":len(calls),"chat":{"id":-100,"type":"supergroup","username":"Promoter0007"}}}
    return {"ok":True,"result":{"username":"promoter_bot"}}
m.tg=fake_tg
m.LINK_COOLDOWN_SECONDS=0

G={"id":-100,"type":"supergroup","username":"Promoter0007"}
def msg(text,uid=5,**extra):
    d={"message_id":1,"chat":G,"from":{"id":uid,"first_name":"U%d"%uid,"username":"user%d"%uid},"text":text}; d.update(extra); return d
def names(): return [c[0] for c in calls]

t=time.time(); m.handle_message(msg("/mylink")); dt=time.time()-t
print("mylink calls:",names(),"time %.2fs"%dt)
assert names()==["createChatInviteLink","sendMessage"]; assert "getChatMember" not in names()
calls.clear(); m.handle_message(msg("/mylink")); print("mylink again (cached):",names()); assert names()==["sendMessage"]

# leaderboard
m.db.referrals.insert_one({"chat_id":"@Promoter0007","inviter_id":5,"inviter_name":"@user5","referred_id":9})
calls.clear(); t=time.time(); m.handle_message(msg("/topinviter",uid=6)); print("top calls:",names(),"%.2fs"%(time.time()-t))
assert names()==["sendMessage"] and "@user5" in calls[0][1]["text"]

# normal chat msg: zero API calls
calls.clear(); m.handle_message(msg("hello everyone")); print("plain msg calls:",names()); assert names()==[]
# link from non-admin -> admin check + delete
calls.clear(); m.handle_message(msg("join t.me/spam")); print("link by member:",names()); assert names()==["getChatMember","deleteMessage"]
# link from admin -> not deleted
calls.clear(); m.handle_message(msg("see https://x.com",uid=222)); print("link by admin:",names()); assert "deleteMessage" not in names()
# admin check cached
calls.clear(); m.handle_message(msg("see https://x.com",uid=222)); assert names()==[]
# admin command by admin and by member
calls.clear(); m.handle_message(msg("/autodelete_time 30",uid=222)); print("admin cmd:",names(), calls[-1][1]["text"]); assert m.get_setting("autodelete_seconds")==30
calls.clear(); m.handle_message(msg("/allmsg_off",uid=7)); print("member admin-cmd:",names()); assert m.get_setting("all_messages_enabled") is True
# owner works without being admin
calls.clear(); m.handle_message(msg("/allmsg_off",uid=111)); assert m.get_setting("all_messages_enabled") is False
# with allmsg off: /mylink from member still works, plain text is deleted
calls.clear(); m.handle_message(msg("/mylink",uid=8)); print("mylink w/ allmsg_off:",names()); assert "sendMessage" in names() and "deleteMessage" not in names()
calls.clear(); m.handle_message(msg("hi",uid=8)); print("text w/ allmsg_off:",names()); assert "deleteMessage" in names()
# anonymous admin
anon={"message_id":2,"chat":G,"from":{"id":1087968824,"is_bot":True,"first_name":"Group"},"sender_chat":G,"text":"/automsg_off"}
calls.clear(); m.handle_message(anon); print("anon admin cmd:",names()); assert m.get_setting("automsg_enabled") is False
anon["text"]="see https://x.com"; calls.clear(); m.handle_message(anon); assert names()==[]
# callback buttons
cb={"id":"c1","data":"generate_mylink","from":{"id":50,"first_name":"Z"},"message":{"message_id":3,"chat":G}}
calls.clear(); m.handle_callback_query(cb); print("callback:",names()); assert names()[0]=="answerCallbackQuery" and "sendMessage" in names()
cb["data"]="show_topinviter"; calls.clear(); m.handle_callback_query(cb); assert "sendMessage" in names()
# command for other bot ignored
m.get_bot_username(); calls.clear(); m.handle_message(msg("/mylink@other_bot")); assert names()==[]; m.handle_message(msg("/mylink@promoter_bot",uid=61)); assert "sendMessage" in names()
# referral tracking before welcome
m.set_setting("welcome_enabled",True)
upd={"chat":G,"old_chat_member":{"status":"left"},"new_chat_member":{"status":"member","user":{"id":77,"first_name":"N"}},"invite_link":{"invite_link":m._link_cache[5]}}
calls.clear(); m.handle_chat_member(upd); print("join:",names()); assert m.db.referrals.count_documents({"chat_id":"@Promoter0007","inviter_id":5})==2
# pacing doesn't serialize threads under a lock
import threading
ts=[];t=time.time()
for i in range(6): 
    th=threading.Thread(target=m._pace_send,args=(i,)); th.start(); ts.append(th)
[x.join() for x in ts]; print("6 different chats paced in %.2fs"%(time.time()-t)); assert time.time()-t<0.5
print("ALL TESTS PASSED")
