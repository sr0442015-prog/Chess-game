// Cloudflare Worker: /ws?room=xxxx で部屋ごとの Durable Object に接続する
// /api/rooms は公開部屋の一覧(Lobby)を返す
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === "/ws") {
      const room = u.searchParams.get("room") || "";
      if (!/^[a-z0-9]{4,12}$/.test(room)) return new Response("bad room", { status: 400 });
      return env.ROOM.get(env.ROOM.idFromName(room)).fetch(req);
    }
    if (u.pathname === "/api/rooms") {
      return env.LOBBY.get(env.LOBBY.idFromName("lobby")).fetch("https://lobby/");
    }
    return env.ASSETS.fetch(req); // public/index.html を配信
  },
};

// 公開部屋の一覧を持つ Durable Object(全体で1つ)
export class Lobby {
  constructor(ctx, env) {
    this.ctx = ctx;
  }

  async fetch(req) {
    const rooms = (await this.ctx.storage.get("rooms")) || {};
    if (req.method === "POST") {
      const d = await req.json();
      if (d.pub && (d.w || d.b)) rooms[d.room] = { n: (d.w ? 1 : 0) + (d.b ? 1 : 0), t: Date.now() };
      else delete rooms[d.room];
      await this.ctx.storage.put("rooms", rooms);
      return new Response("ok");
    }
    const old = Date.now() - 12 * 3600 * 1000;
    const list = Object.entries(rooms)
      .filter(([, v]) => v.t > old)
      .sort((a, b) => b[1].t - a[1].t)
      .slice(0, 20)
      .map(([room, v]) => ({ room, n: v.n }));
    return Response.json(list, { headers: { "cache-control": "no-store" } });
  }
}

// 1部屋 = 1つの Durable Object。先着で白・黒、3人目以降は観戦(s)
export class Room {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }

  live() {
    return this.ctx.getWebSockets().filter((s) => s.readyState === 1);
  }

  counts(except) {
    const c = this.live().filter((s) => s !== except).map((s) => s.deserializeAttachment()?.color);
    return { w: c.includes("w"), b: c.includes("b") };
  }

  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const ws of this.live()) if (ws !== except) { try { ws.send(s); } catch {} }
  }

  // 在室状況を部屋の全員に送り、公開部屋ならロビーにも知らせる
  async sendPresence(except) {
    const c = this.counts(except);
    let pub = !!(await this.ctx.storage.get("pub"));
    if (!c.w && !c.b && pub) { // 誰もいなくなったら公開を解除
      pub = false;
      await this.ctx.storage.put("pub", false);
    }
    this.broadcast({ t: "p", ...c, pub }, except);
    const room = await this.ctx.storage.get("name");
    if (room) {
      try {
        await this.env.LOBBY.get(this.env.LOBBY.idFromName("lobby")).fetch("https://lobby/", {
          method: "POST",
          body: JSON.stringify({ room, pub, ...c }),
        });
      } catch {}
    }
  }

  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
    await this.ctx.storage.put("name", new URL(req.url).searchParams.get("room"));
    const taken = this.live().map((s) => s.deserializeAttachment()?.color);
    const color = !taken.includes("w") ? "w" : !taken.includes("b") ? "b" : "s";
    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ color });
    const moves = (await this.ctx.storage.get("moves")) || [];
    server.send(JSON.stringify({ t: "hello", color, moves }));
    await this.sendPresence();
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
    } else if (m.t === "pub" && color !== "s") {
      await this.ctx.storage.put("pub", !!m.v);
      await this.sendPresence();
    }
  }

  async webSocketClose(ws, code) {
    try { ws.close(code); } catch {}
    await this.sendPresence(ws);
  }

  async webSocketError(ws) {
    await this.sendPresence(ws);
  }
}
