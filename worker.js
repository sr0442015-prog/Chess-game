// Cloudflare Worker: /ws?room=xxxx で部屋ごとの Durable Object に接続する
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === "/ws") {
      const room = u.searchParams.get("room") || "";
      if (!/^[a-z0-9]{4,12}$/.test(room)) return new Response("bad room", { status: 400 });
      return env.ROOM.get(env.ROOM.idFromName(room)).fetch(req);
    }
    return env.ASSETS.fetch(req); // public/index.html を配信
  },
};

// 1部屋 = 1つの Durable Object。先着で白・黒、3人目以降は観戦(s)
export class Room {
  constructor(ctx, env) {
    this.ctx = ctx;
  }

  live() {
    return this.ctx.getWebSockets().filter((s) => s.readyState === 1);
  }

  presence() {
    const c = this.live().map((s) => s.deserializeAttachment()?.color);
    return { t: "p", w: c.includes("w"), b: c.includes("b") };
  }

  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const ws of this.live()) if (ws !== except) ws.send(s);
  }

  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
    const taken = this.live().map((s) => s.deserializeAttachment()?.color);
    const color = !taken.includes("w") ? "w" : !taken.includes("b") ? "b" : "s";
    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ color });
    const moves = (await this.ctx.storage.get("moves")) || [];
    server.send(JSON.stringify({ t: "hello", color, moves }));
    this.broadcast(this.presence());
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, data) {
    let m;
    try { m = JSON.parse(data); } catch { return; }
    const color = ws.deserializeAttachment()?.color;
    if (m.t === "move") {
      const moves = (await this.ctx.storage.get("moves")) || [];
      const turn = moves.length % 2 ? "b" : "w";
      if (color !== turn || typeof m.mv !== "string" || !/^[a-h][1-8][a-h][1-8][qrbn]?$/.test(m.mv)) {
        ws.send(JSON.stringify({ t: "sync", moves })); // 不正なら正しい状態に戻す
        return;
      }
      moves.push(m.mv);
      await this.ctx.storage.put("moves", moves);
      this.broadcast({ t: "move", mv: m.mv }, ws);
    } else if (m.t === "new" && color !== "s") {
      await this.ctx.storage.put("moves", []);
      this.broadcast({ t: "new" });
    } else if (m.t === "resign" && color !== "s") {
      this.broadcast({ t: "resign", by: color });
    }
  }

  async webSocketClose(ws, code) {
    try { ws.close(code); } catch {}
    this.broadcast(this.presence());
  }

  async webSocketError(ws) {
    this.broadcast(this.presence());
  }
}
