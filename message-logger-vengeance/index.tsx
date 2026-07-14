import { definePlugin } from "@revenge-mod/plugins";
import { findByPropsLazy } from "@revenge-mod/modules";
import { filters } from "@revenge-mod/modules/webpack";

// ─── IndexedDB Persistent Store ─────────────────────────────────────────────
const DB_NAME = "RevengeMessageLog";
const DB_VERSION = 2;
const STORE_NAME = "messages";

const DB = {
    open(): Promise<IDBDatabase> {
        return new Promise((resolve, reject) => {
            const req = indexedDB.open(DB_NAME, DB_VERSION);
            req.onupgradeneeded = (e) => {
                const db = (e.target as IDBRequest).result as IDBDatabase;
                if (!db.objectStoreNames.contains(STORE_NAME)) {
                    const store = db.createObjectStore(STORE_NAME, { keyPath: "id" });
                    store.createIndex("channelId", "channelId", { unique: false });
                    store.createIndex("authorId", "authorId", { unique: false });
                    store.createIndex("guildId", "guildId", { unique: false });
                    store.createIndex("deleted", "deleted", { unique: false });
                    store.createIndex("timestamp", "timestamp", { unique: false });
                }
            };
            req.onsuccess = (e) => resolve((e.target as IDBRequest).result as IDBDatabase);
            req.onerror = (e) => reject((e.target as IDBRequest).error);
        });
    },
    async put(msg: any): Promise<void> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            tx.objectStore(STORE_NAME).put(msg);
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    },
    async get(id: string): Promise<any | null> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readonly");
            const req = tx.objectStore(STORE_NAME).get(id);
            req.onsuccess = () => resolve(req.result || null);
            req.onerror = () => reject(req.error);
        });
    },
    async getAll(): Promise<any[]> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readonly");
            const req = tx.objectStore(STORE_NAME).getAll();
            req.onsuccess = () => resolve(req.result || []);
            req.onerror = () => reject(req.error);
        });
    },
    async getDeleted(limit = 1000): Promise<any[]> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readonly");
            const index = tx.objectStore(STORE_NAME).index("deleted");
            const results: any[] = [];
            const req = index.openCursor(IDBKeyRange.only(true), "prev");
            req.onsuccess = (e) => {
                const cursor = (e.target as IDBRequest).result as IDBCursorWithValue | null;
                if (cursor && results.length < limit) {
                    results.push(cursor.value);
                    cursor.continue();
                } else resolve(results);
            };
            req.onerror = () => reject(req.error);
        });
    },
    async getByChannel(channelId: string, limit = 500): Promise<any[]> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readonly");
            const index = tx.objectStore(STORE_NAME).index("channelId");
            const results: any[] = [];
            const req = index.openCursor(IDBKeyRange.only(channelId), "prev");
            req.onsuccess = (e) => {
                const cursor = (e.target as IDBRequest).result as IDBCursorWithValue | null;
                if (cursor && results.length < limit) {
                    results.push(cursor.value);
                    cursor.continue();
                } else resolve(results);
            };
            req.onerror = () => reject(req.error);
        });
    },
    async searchByAuthor(authorId: string, limit = 200): Promise<any[]> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readonly");
            const index = tx.objectStore(STORE_NAME).index("authorId");
            const results: any[] = [];
            const req = index.openCursor(IDBKeyRange.only(authorId), "prev");
            req.onsuccess = (e) => {
                const cursor = (e.target as IDBRequest).result as IDBCursorWithValue | null;
                if (cursor && results.length < limit) {
                    results.push(cursor.value);
                    cursor.continue();
                } else resolve(results);
            };
            req.onerror = () => reject(req.error);
        });
    },
    async prune(maxItems = 10000): Promise<number> {
        const all = await this.getAll();
        if (all.length <= maxItems) return 0;
        all.sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime());
        const toDelete = all.slice(maxItems);
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            const store = tx.objectStore(STORE_NAME);
            for (const msg of toDelete) store.delete(msg.id);
            tx.oncomplete = () => resolve(toDelete.length);
            tx.onerror = () => reject(tx.error);
        });
    },
    async exportAll(): Promise<any[]> {
        return await this.getAll();
    },
    async importAll(msgs: any[]): Promise<void> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_NAME, "readwrite");
            const store = tx.objectStore(STORE_NAME);
            for (const msg of msgs) store.put(msg);
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    },
};

// ─── Discord Webpack Module References ──────────────────────────────────────
const FluxDispatcher = findByPropsLazy("dispatch", "subscribe");
const UserStore = findByPropsLazy("getCurrentUser");
const ChannelStore = findByPropsLazy("getChannel");
const GuildStore = findByPropsLazy("getGuild");

function getCurrentUserId(): string {
    try {
        return UserStore?.getCurrentUser()?.id || "unknown";
    } catch {
        return "unknown";
    }
}

function getUserTag(user: any): string {
    if (!user) return "Unknown#0000";
    return `${user.username}#${user.discriminator || "0000"}`;
}

// ─── Message Normalization ──────────────────────────────────────────────────
function normalizeMessage(msg: any): any {
    return {
        id: msg.id,
        channelId: msg.channel_id || msg.channelId,
        guildId: msg.guild_id || msg.guildId || null,
        authorId: msg.author?.id || msg.authorId,
        authorTag: getUserTag(msg.author),
        authorAvatar: msg.author?.avatar,
        content: msg.content || "",
        timestamp: msg.timestamp || new Date().toISOString(),
        editedAt: msg.editedTimestamp || null,
        attachments: (msg.attachments || []).map((a: any) => ({
            id: a.id,
            url: a.url,
            filename: a.filename,
            size: a.size,
            contentType: a.content_type,
        })),
        embeds: (msg.embeds || []).map((e: any) => ({
            type: e.type,
            title: e.title,
            description: e.description,
            url: e.url,
        })),
        deleted: false,
        deletedAt: null,
        editHistory: [],
        loggedAt: new Date().toISOString(),
    };
}

// ─── Plugin Definition ──────────────────────────────────────────────────────
export default definePlugin({
    id: "xyz.revenge.message-logger-vengeance",
    name: "MessageLoggerVengeance",
    description:
        "Persistently logs ALL messages and recovers deleted/edited messages even after Discord restarts. IndexedDB-backed. See what was said before deletion — always.",
    version: "1.0.0",
    authors: [{ name: "HackerAI Red Team" }],

    settings: {
        maxMessages: {
            type: "number",
            label: "Max stored messages",
            description: "Maximum number of messages to keep in IndexedDB. Oldest are pruned first.",
            default: 10000,
            min: 100,
            max: 100000,
        },
        ignoreBots: {
            type: "boolean",
            label: "Ignore bot messages",
            default: false,
        },
        ignoreSelf: {
            type: "boolean",
            label: "Ignore your own messages",
            default: false,
        },
        logEdits: {
            type: "boolean",
            label: "Log message edits",
            description: "Track edit history for messages",
            default: true,
        },
        showInChat: {
            type: "boolean",
            label: "Show deleted message indicator in chat",
            description: "Shows a faint 'deleted' label on messages that were deleted (stays visible until reload)",
            default: true,
        },
    },

    start() {
        this._handlers = new Set();
        this._messageCache = new Map();

        // ── Intercept MESSAGE_CREATE ───────────────────────────────────
        const createHandler = (event: any) => {
            if (event.type !== "MESSAGE_CREATE" || !event.message) return;
            const msg = event.message;
            if (
                (this.settings.ignoreBots && msg.author?.bot) ||
                (this.settings.ignoreSelf && msg.author?.id === getCurrentUserId())
            )
                return;

            const record = normalizeMessage(msg);
            this._messageCache.set(msg.id, record);
            DB.put(record).catch(() => {});
            DB.prune(this.settings.maxMessages).catch(() => {});
        };

        // ── Intercept MESSAGE_DELETE ───────────────────────────────────
        const deleteHandler = (event: any) => {
            if (event.type !== "MESSAGE_DELETE" || !event.messageId) return;

            const msgId = event.messageId;
            const cached = this._messageCache.get(msgId);

            if (cached) {
                cached.deleted = true;
                cached.deletedAt = new Date().toISOString();
                this._messageCache.set(msgId, cached);
                DB.put(cached).catch(() => {});
            } else {
                // Try to fetch from DB and mark
                DB.get(msgId).then((stored) => {
                    if (stored) {
                        stored.deleted = true;
                        stored.deletedAt = new Date().toISOString();
                        this._messageCache.set(msgId, stored);
                        DB.put(stored).catch(() => {});
                    }
                }).catch(() => {});
            }
        };

        // ── Intercept MESSAGE_DELETE_BULK ──────────────────────────────
        const bulkDeleteHandler = (event: any) => {
            if (event.type !== "MESSAGE_DELETE_BULK" || !event.ids) return;
            for (const id of event.ids) {
                const cached = this._messageCache.get(id);
                if (cached) {
                    cached.deleted = true;
                    cached.deletedAt = new Date().toISOString();
                    this._messageCache.set(id, cached);
                    DB.put(cached).catch(() => {});
                } else {
                    DB.get(id).then((stored) => {
                        if (stored) {
                            stored.deleted = true;
                            stored.deletedAt = new Date().toISOString();
                            this._messageCache.set(id, stored);
                            DB.put(stored).catch(() => {});
                        }
                    }).catch(() => {});
                }
            }
        };

        // ── Intercept MESSAGE_UPDATE ───────────────────────────────────
        const updateHandler = (event: any) => {
            if (event.type !== "MESSAGE_UPDATE" || !event.message) return;
            if (!this.settings.logEdits) return;

            const msg = event.message;
            if (!msg.id || !msg.content) return;

            const cached = this._messageCache.get(msg.id);
            if (cached) {
                if (cached.content !== msg.content) {
                    cached.editHistory = cached.editHistory || [];
                    cached.editHistory.push({
                        content: cached.content,
                        timestamp: new Date().toISOString(),
                    });
                    cached.content = msg.content;
                    cached.editedAt = new Date().toISOString();
                    this._messageCache.set(msg.id, cached);
                    DB.put(cached).catch(() => {});
                }
            } else {
                DB.get(msg.id).then((stored) => {
                    if (stored && stored.content !== msg.content) {
                        stored.editHistory = stored.editHistory || [];
                        stored.editHistory.push({
                            content: stored.content,
                            timestamp: new Date().toISOString(),
                        });
                        stored.content = msg.content;
                        stored.editedAt = new Date().toISOString();
                        this._messageCache.set(msg.id, stored);
                        DB.put(stored).catch(() => {});
                    }
                }).catch(() => {});
            }
        };

        // Subscribe to Flux events
        if (FluxDispatcher?.subscribe) {
            FluxDispatcher.subscribe("MESSAGE_CREATE", createHandler);
            FluxDispatcher.subscribe("MESSAGE_DELETE", deleteHandler);
            FluxDispatcher.subscribe("MESSAGE_DELETE_BULK", bulkDeleteHandler);
            FluxDispatcher.subscribe("MESSAGE_UPDATE", updateHandler);
            this._handlers.add(() => {
                FluxDispatcher.unsubscribe("MESSAGE_CREATE", createHandler);
                FluxDispatcher.unsubscribe("MESSAGE_DELETE", deleteHandler);
                FluxDispatcher.unsubscribe("MESSAGE_DELETE_BULK", bulkDeleteHandler);
                FluxDispatcher.unsubscribe("MESSAGE_UPDATE", updateHandler);
            });
        }

        // ── Patch message rendering to show deleted indicator ──────────
        if (this.settings.showInChat) {
            this._patchDeletedMessageRendering();
        }

        console.log("[MessageLoggerVengeance] Started — now logging all messages to IndexedDB");
    },

    stop() {
        for (const cleanup of this._handlers) {
            try {
                cleanup();
            } catch {}
        }
        this._handlers.clear();
        this._messageCache.clear();
        console.log("[MessageLoggerVengeance] Stopped");
    },

    // ── Render Patch: Show deleted indicators ──────────────────────────
    _patchDeletedMessageRendering() {
        try {
            const MessageStore = findByPropsLazy("getMessage", "getMessages");
            if (!MessageStore?.getMessage) return;

            const origGetMessage = MessageStore.getMessage;
            const cache = this._messageCache;
            MessageStore.getMessage = function (...args: any[]) {
                const msg = origGetMessage.apply(this, args);
                if (msg && cache.has(msg.id)) {
                    const logged = cache.get(msg.id);
                    if (logged?.deleted) {
                        // Add a visual flag to the message object
                        return { ...msg, deleted: true, _mlvDeleted: true };
                    }
                }
                return msg;
            };
            this._handlers.add(() => {
                MessageStore.getMessage = origGetMessage;
            });
        } catch (e) {
            console.warn("[MessageLoggerVengeance] Could not patch message rendering:", e);
        }
    },

    // ── Commands / Actions exposed via settings ────────────────────────
    actions: {
        async viewDeletedLogs() {
            const deletedMsgs = await DB.getDeleted(500);
            const total = await DB.getAll();
            // Trigger a modal/alert with the data
            const data = JSON.stringify(
                {
                    totalLogged: total.length,
                    totalDeleted: deletedMsgs.length,
                    deletedMessages: deletedMsgs.map((m) => ({
                        id: m.id,
                        author: m.authorTag,
                        content: m.content,
                        deletedAt: m.deletedAt,
                        channelId: m.channelId,
                        guildId: m.guildId,
                        timestamp: m.timestamp,
                        attachments: m.attachments?.length || 0,
                        editHistory: m.editHistory?.length || 0,
                    })),
                },
                null,
                2
            );
            // Use a simple alert or modal
            alert(`📋 MessageLoggerVengeance - Deleted Messages\n\nTotal logged: ${total.length}\nDeleted: ${deletedMsgs.length}\n\nCheck console for full JSON`);
            console.log("[MessageLoggerVengeance] Full log data:", JSON.parse(data));
        },

        async exportLogs() {
            const all = await DB.exportAll();
            const blob = new Blob([JSON.stringify(all, null, 2)], { type: "application/json" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = `revenge-message-log-${Date.now()}.json`;
            a.click();
            URL.revokeObjectURL(url);
        },

        async clearLogs() {
            if (!confirm("Delete ALL logged messages from IndexedDB? This cannot be undone.")) return;
            const db = await DB.open();
            const tx = db.transaction(STORE_NAME, "readwrite");
            tx.objectStore(STORE_NAME).clear();
            this._messageCache.clear();
            alert("All logs cleared.");
        },

        async searchByUser(userId: string) {
            if (!userId) {
                userId = prompt("Enter user ID to search for:");
                if (!userId) return;
            }
            const results = await DB.searchByAuthor(userId);
            alert(`Found ${results.length} messages from user ${userId}\n\nCheck console for details.`);
            console.log(`[MessageLoggerVengeance] Messages from ${userId}:`, results);
        },

        async searchByChannel(channelId: string) {
            if (!channelId) {
                channelId = prompt("Enter channel ID:");
                if (!channelId) return;
            }
            const results = await DB.getByChannel(channelId);
            alert(`Found ${results.length} messages in channel ${channelId}\n\nCheck console for details.`);
            console.log(`[MessageLoggerVengeance] Messages in ${channelId}:`, results);
        },

        async getStats() {
            const total = await DB.getAll();
            const deleted = await DB.getDeleted();
            alert(
                `📊 MessageLoggerVengeance Stats\n\n` +
                `Total messages logged: ${total.length}\n` +
                `Deleted messages recovered: ${deleted.length}\n` +
                `Message cache size: ${this._messageCache.size}`
            );
        },
    },
});
