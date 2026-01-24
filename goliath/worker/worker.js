export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Bearer token (stats only)
    const auth = request.headers.get("authorization") || "";
    const isAuthed = env.STATS_TOKEN && auth === `Bearer ${env.STATS_TOKEN}`;

    if (url.pathname === "/log" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
      } catch {
        return new Response("bad json", { status: 400 });
      }

      const ts = (body.ts || new Date().toISOString()).toString();
      const ad_id = (body.ad_id || "").toString().slice(0, 120);
      const genre = (body.genre || "").toString().slice(0, 80);
      const page_id = (body.page_id || "").toString().slice(0, 80);
      const page_url = (body.page_url || "").toString().slice(0, 300);

      if (!ad_id) return new Response("missing ad_id", { status: 400 });

      // 日単位 key（UTC）
      const day = ts.slice(0, 10); // YYYY-MM-DD
      const key = `click:${day}:${ad_id}`;

      // KV: count increment
      const cur = parseInt((await env.CLICKS.get(key)) || "0", 10) || 0;
      await env.CLICKS.put(key, String(cur + 1));

      // 参考用（必要なら後で使う）: ad→genre, ad→page の軽い最新記録（個人情報なし）
      await env.CLICKS.put(`meta:${ad_id}`, JSON.stringify({ genre, page_id, page_url }), { expirationTtl: 60 * 60 * 24 * 30 });

      return new Response("ok", { status: 200 });
    }

    if (url.pathname === "/stats" && request.method === "GET") {
      if (!isAuthed) return new Response("unauthorized", { status: 401 });

      const days = Math.max(1, Math.min(30, parseInt(url.searchParams.get("days") || "7", 10) || 7));
      const now = new Date();
      const by_ad_id = {};

      // 直近N日ぶんを走査（キーは click:YYYY-MM-DD:ad_id）
      for (let i = 0; i < days; i++) {
        const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
        d.setUTCDate(d.getUTCDate() - i);
        const day = d.toISOString().slice(0, 10);

        // prefix list
        const prefix = `click:${day}:`;
        let cursor;
        do {
          const res = await env.CLICKS.list({ prefix, cursor });
          cursor = res.cursor;

          for (const k of res.keys) {
            const ad_id = k.name.slice(prefix.length);
            const v = parseInt((await env.CLICKS.get(k.name)) || "0", 10) || 0;
            by_ad_id[ad_id] = (by_ad_id[ad_id] || 0) + v;
          }
        } while (cursor);
      }

      return Response.json({ days, by_ad_id });
    }

    return new Response("not found", { status: 404 });
  },
};
