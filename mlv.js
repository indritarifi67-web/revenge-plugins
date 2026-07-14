// == MessageLoggerVengeance - Persistent Deleted Message Logger for Revenge ==
// Install URL: https://raw.githubusercontent.com/indritarifi67-web/revenge-plugins/main/mlv.js

const DB = {
    _db: null,
    async open() {
        if (this._db) return this._db;
        return new Promise((resolve, reject) => {
            const req = indexedDB.open("RevengeMLV", 1);
            req.onupgradeneeded = (e) => {
                const db = e.target.result;
                if (!db.objectStoreNames.contains("msgs")) {
                    const s = db.createObjectStore("msgs", { keyPath: "id" });
                    s.createIndex("ch", "ch", { unique: false });
                    s.createIndex("au", "au", { unique: false });
                    s.createIndex("del", "del", { unique: false });
                }
            };
            req.onsuccess = (e) => { this._db = e.target.result; resolve(this._db); };
            req.onerror = (e) => reject(e.target.error);
        });
    },
    async put(msg) { const db = await this.open(); return new Promise((res, rej) => { const t = db.transaction("msgs", "readwrite"); t.objectStore("msgs").put(msg); t.oncomplete = () => res(); t.onerror = () => rej(t.error); }); },
    async get(id) { const db = await this.open(); return new Promise((res, rej) => { const r = db.transaction("msgs").objectStore("msgs").get(id); r.onsuccess = () => res(r.result || null); r.onerror = () => rej(r.error); }); },
    async getAll() { const db = await this.open(); return new Promise((res, rej) => { const r = db.transaction("msgs").objectStore("msgs").getAll(); r.onsuccess = () => res(r.result || []); r.onerror = () => rej(r.error); }); },
    async getDeleted(limit) {
        limit = limit || 1000;
        const db = await this.open();
        return new Promise((res, rej) => {
            const results = [];
            const r = db.transaction("msgs").objectStore("msgs").index("del").openCursor(IDBKeyRange.only(1), "prev");
            r.onsuccess = (e) => { const c = e.target.result; if (c && results.length < limit) { results.push(c.value); c.continue(); } else res(results); };
            r.onerror = () => rej(r.error);
        });
    },
    async prune(max) {
        const all = await this.getAll();
        if (all.length <= max) return;
        all.sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0));
        const kill = all.slice(max);
        const db = await this.open();
        const t = db.transaction("msgs", "readwrite");
        const s = t.objectStore("msgs");
        kill.forEach(m => s.delete(m.id));
    }
};

const Fl Drain = window?.webpackChunkdiscord_app?.push([[], {}, (e) => e])?.c;
function findModule(...props) {
    for (const k of Object.keys(Fl Drain || {})) {
        const mod = Fl Drain[k]?.exports;
        if (mod && props.every(p => mod[p] !== undefined)) return mod;
    }
    return null;
}

let dispatcher, userStore, userId = "0";
let cache = new Map();
let handlers = [];

function norm(msg) {
    return {
        id: msg.id,
        ch: msg.channel_id || msg.channelId,
        gu: msg.guild_id || msg.guildId || null,
        au: msg.author?.id || msg.authorId,
        tag: msg.author ? `${msg.author.username}#${msg.author.discriminator || "0000"}` : "Unknown",
        con: msg.content || "",
        ts: msg.timestamp || new Date().toISOString(),
        at: (msg.attachments || []).map(a => ({ id: a.id, url: a.url, fn: a.filename })),
        del: 0,
        delAt: null,
        logAt: new Date().toISOString()
    };
}

function start() {
    dispatcher = findModule("dispatch", "subscribe");
    userStore = findModule("getCurrentUser");
    if (userStore?.getCurrentUser) userId = userStore.getCurrentUser()?.id || "0";

    if (!dispatcher) { console.warn("[MLV] No dispatcher found"); return; }

    const onMsg = (e) => {
        if (e.type !== "MESSAGE_CREATE" || !e.message) return;
        const msg = e.message;
        if (msg.author?.bot || msg.author?.id === userId) return;
        const rec = norm(msg);
        cache.set(msg.id, rec);
        DB.put(rec);
        DB.prune(10000);
    };
    const onDel = (e) => {
        if (e.type !== "MESSAGE_DELETE" || !e.messageId) return;
        const id = e.messageId;
        const c = cache.get(id);
        if (c) { c.del = 1; c.delAt = new Date().toISOString(); cache.set(id, c); DB.put(c); }
        else { DB.get(id).then(s => { if (s) { s.del = 1; s.delAt = new Date().toISOString(); cache.set(id, s); DB.put(s); } }); }
    };
    const onBulk = (e) => {
        if (e.type !== "MESSAGE_DELETE_BULK" || !e.ids) return;
        e.ids.forEach(id => {
            const c = cache.get(id);
            if (c) { c.del = 1; c.delAt = new Date().toISOString(); cache.set(id, c); DB.put(c); }
            else { DB.get(id).then(s => { if (s) { s.del = 1; s.delAt = new Date().toISOString(); cache.set(id, s); DB.put(s); } }); }
        });
    };

    dispatcher.subscribe("MESSAGE_CREATE", onMsg);
    dispatcher.subscribe("MESSAGE_DELETE", onDel);
    dispatcher.subscribe("MESSAGE_DELETE_BULK", onBulk);

    handlers = [
        () => dispatcher.unsubscribe("MESSAGE_CREATE", onMsg),
        () => dispatcher.unsubscribe("MESSAGE_DELETE", onDel),
        () => dispatcher.unsubscribe("MESSAGE_DELETE_BULK", onBulk)
    ];

    console.log("[MLV] Started - logging all messages to IndexedDB");
}

function stop() {
    handlers.forEach(f => { try { f(); } catch {} });
    handlers = [];
    cache.clear();
    console.log("[MLV] Stopped");
}

// Self-execute if loaded as a plugin
try {
    if (typeof revenge !== "undefined" && revenge?.plugins) {
        const plugin = revenge.plugins.register({
            name: "MessageLoggerVengeance",
            description: "Persistently logs all messages and recovers deleted ones even after restart. IndexedDB-backed.",
            version: "1.0.0",
            author: "HackerAI",
            start,
            stop
        });
        console.log("[MLV] Plugin registered with Revenge");
    } else {
        // Fallback: auto-start
        setTimeout(start, 3000);
    }
} catch (e) {
    console.warn("[MLV] Error registering:", e);
    setTimeout(start, 5000);
    }
